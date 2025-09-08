package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-faker/faker/v4"
	_ "github.com/go-sql-driver/mysql"
)

type DBConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	DBName   string `json:"dbname"`
}

type ColumnDef struct {
	Name      string
	Type      string
	Size      int
	FakerRule string
}

type TableDef struct {
	Name     string
	Columns  []ColumnDef
	PKColumn string
}

type SQLJob struct {
	Operation string
	Data      []interface{}
	TargetID  int64
}

type IDTracker struct {
	mu  sync.Mutex
	ids []int64
}

func (t *IDTracker) Add(id int64) {
	t.mu.Lock()
	t.ids = append(t.ids, id)
	t.mu.Unlock()
}

func (t *IDTracker) GetRandomID() (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.ids) == 0 {
		return 0, false
	}
	randomIndex := rand.Intn(len(t.ids))
	return t.ids[randomIndex], true
}

type ProgressTracker struct {
	mu          sync.Mutex
	counts      map[int]int64
	countBefore int64
}

func NewProgressTracker() *ProgressTracker {
	return &ProgressTracker{
		counts: make(map[int]int64),
	}
}

func (p *ProgressTracker) Increment(workerID int) {
	p.mu.Lock()
	p.counts[workerID]++
	p.mu.Unlock()
}

func (p *ProgressTracker) GetReport() (map[int]int64, int64, int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	report := make(map[int]int64, len(p.counts))
	var total int64
	for id, count := range p.counts {
		report[id] = count
		total += count
	}
	avg := int64(total - p.countBefore)
	p.countBefore = total
	return report, total, avg
}

const (
	OpInsert = "INSERT"
	OpUpdate = "UPDATE"
)

func main() {

	threadCount := flag.Int("threads", 10, "Number of GoRoutines to spawn")
	totalOps := flag.Int("ops", 10000, "Total number of operations to perform")
	ddlPath := flag.String("ddl", "", "Path to the DDL (.sql) file")
	configPath := flag.String("config", "", "Path to the database config (.json) file")
	mode := flag.String("mode", "insert", "Operation mode: 'insert' or 'mixed'")
	updateRatio := flag.Float64("update-ratio", 0.3, "Ratio of UPDATEs in 'mixed' mode (e.g., 0.3 for 30%)")
	flag.Parse() // Hapus flag verbose karena digantikan oleh progress report

	if *ddlPath == "" || *configPath == "" {
		fmt.Println("Error: Both -ddl and -config arguments are required.")
		flag.Usage()
		os.Exit(1)
	}
	if *mode != "insert" && *mode != "mixed" {
		log.Fatalf("Error: Invalid mode '%s'. Alowed modes are 'insert' or 'mixed'.", *mode)
	}

	log.Println("Starting data simulator...")

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	db, err := connectDB(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()
	log.Println("Successfully connected to database.")

	table, err := parseDDLFile(*ddlPath)
	if err != nil {
		log.Fatalf("Failed to parse DDL file: %v", err)
	}

	if table.PKColumn == "" {
		log.Println("Warning: No AUTO_INCREMENT primary key found. UPDATE operations will be disabled.")
		if *mode == "mixed" {
			*mode = "insert"
			log.Println("Mode has been forced to 'insert'.")
		}
	}
	log.Printf("Mode: %s | Worker threads: %d | Total operations: %d", *mode, *threadCount, *totalOps)

	// **SETUP UNTUK LAPORAN PROGRES**
	progress := NewProgressTracker()
	ticker := time.NewTicker(1 * time.Second)
	doneReporter := make(chan bool)
	var reporterWg sync.WaitGroup

	reporterWg.Add(1)
	go func() {
		defer reporterWg.Done()
		for {
			select {
			case <-doneReporter:
				return
			case <-ticker.C:
				report, total, avg := progress.GetReport()
				var workerIDs []int
				for id := range report {
					workerIDs = append(workerIDs, id)
				}
				sort.Ints(workerIDs)
				log.Printf("Total: %d / %d ops | QPS: %d", total, *totalOps, avg)
			}
		}
	}()

	// Menjalankan workers
	var workerWg sync.WaitGroup
	jobChannel := make(chan SQLJob, *threadCount*2)
	idTracker := &IDTracker{}
	for i := 0; i < *threadCount; i++ {
		workerWg.Add(1)
		go worker(i+1, &workerWg, db, jobChannel, progress, idTracker, table)
	}
	startDate := time.Now()
	for i := 0; i < *totalOps; i++ {
		var job SQLJob
		shouldUpdate := (table.PKColumn != "" && *mode == "mixed" && rand.Float64() < *updateRatio)
		if shouldUpdate {
			if targetID, ok := idTracker.GetRandomID(); ok {
				record, _ := generateFakeData(table)
				job = SQLJob{Operation: OpUpdate, Data: record, TargetID: targetID}
			} else {
				shouldUpdate = false
			}
		}
		if !shouldUpdate {
			record, _ := generateFakeData(table)
			job = SQLJob{Operation: OpInsert, Data: record}
		}
		jobChannel <- job
	}
	close(jobChannel)

	// Menunggu semua worker selesai
	workerWg.Wait()

	endDate := time.Now()

	// Memberhentikan reporter
	ticker.Stop()
	doneReporter <- true
	reporterWg.Wait()

	_, finalTotal, _ := progress.GetReport()
	elapsedTime := endDate.Sub(startDate)
	log.Printf("Final Report: A total of %d operations were completed.", finalTotal)
	log.Printf("Total time taken: %s", elapsedTime)
	log.Println("All workers finished. Process complete.")
}

func worker(id int, wg *sync.WaitGroup, db *sql.DB, jobChannel <-chan SQLJob, progress *ProgressTracker, idTracker *IDTracker, table TableDef) {
	defer wg.Done()

	insertStmt, err := db.Prepare(buildInsertStatement(table))
	if err != nil {
		log.Printf("Worker %d | Failed to prepare insert statement: %v", id, err)
		return
	}
	defer insertStmt.Close()

	var updateStmt *sql.Stmt
	if table.PKColumn != "" {
		updateStmt, err = db.Prepare(buildUpdateStatement(table))
		if err != nil {
			log.Printf("Worker %d | Failed to prepare update statement: %v", id, err)
			return
		}
		defer updateStmt.Close()
	}

	for job := range jobChannel {
		var res sql.Result
		var execErr error
		switch job.Operation {
		case OpInsert:
			res, execErr = insertStmt.Exec(job.Data...)
			if execErr == nil && table.PKColumn != "" {
				newID, _ := res.LastInsertId()
				idTracker.Add(newID)
			}
		case OpUpdate:
			args := append(job.Data, job.TargetID)
			_, execErr = updateStmt.Exec(args...)
		}

		if execErr != nil {
			log.Printf("Worker %d | Error: %v", id, execErr)
		} else {
			progress.Increment(id)
		}
	}
}

func parseDDLFile(path string) (TableDef, error) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return TableDef{}, err
	}
	return parseDDL(string(content))
}

func parseDDL(ddl string) (TableDef, error) {
	var table TableDef
	tableNameRegex := regexp.MustCompile("(?i)CREATE TABLE `?(\\w+)`?")
	matches := tableNameRegex.FindStringSubmatch(ddl)
	if len(matches) < 2 {
		return table, fmt.Errorf("could not find table name in DDL")
	}
	table.Name = matches[1]

	colsBlockRegex := regexp.MustCompile(`\(([\s\S]*)\)`)
	colsBlock := colsBlockRegex.FindStringSubmatch(ddl)
	if len(colsBlock) < 2 {
		return table, fmt.Errorf("could not find columns block")
	}

	lines := strings.Split(colsBlock[1], "\n")
	fakerRuleRegex := regexp.MustCompile(`/\*TYPE:\s*(.*?)\s*\*/`)
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		trimmedLine = strings.TrimRight(trimmedLine, ",")
		if trimmedLine == "" || strings.HasPrefix(strings.ToUpper(trimmedLine), "PRIMARY") ||
			strings.HasPrefix(strings.ToUpper(trimmedLine), "UNIQUE") ||
			strings.HasPrefix(strings.ToUpper(trimmedLine), "KEY") ||
			strings.HasPrefix(strings.ToUpper(trimmedLine), "CONSTRAINT") {
			continue
		}
		parts := strings.Fields(trimmedLine)
		if len(parts) >= 2 {
			colName := strings.Trim(parts[0], "`")
			rawType := parts[1]
			colType := strings.ToLower(strings.Split(rawType, "(")[0])
			colSize := 0
			sizeRegex := regexp.MustCompile(`\((\d+)\)`)
			sizeMatches := sizeRegex.FindStringSubmatch(rawType)
			if len(sizeMatches) > 1 {
				size, err := strconv.Atoi(sizeMatches[1])
				if err == nil {
					colSize = size
				}
			}
			if strings.Contains(strings.ToUpper(trimmedLine), "AUTO_INCREMENT") {
				table.PKColumn = colName
			}
			var fakerRule string
			ruleMatches := fakerRuleRegex.FindStringSubmatch(trimmedLine)
			if len(ruleMatches) > 1 {
				fakerRule = ruleMatches[1]
			}

			if strings.Contains(strings.ToUpper(trimmedLine), "AUTO_INCREMENT") {
				table.PKColumn = colName
			}

			table.Columns = append(table.Columns, ColumnDef{
				Name:      colName,
				Type:      colType,
				Size:      colSize,
				FakerRule: fakerRule,
			})
		}
	}
	return table, nil
}

func buildInsertStatement(table TableDef) string {
	var columnNames, valuePlaceholders []string
	for _, col := range table.Columns {
		if col.Name == table.PKColumn {
			continue
		}
		columnNames = append(columnNames, fmt.Sprintf("`%s`", col.Name))
		valuePlaceholders = append(valuePlaceholders, "?")
	}
	return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		table.Name, strings.Join(columnNames, ", "), strings.Join(valuePlaceholders, ", "))
}

func buildUpdateStatement(table TableDef) string {
	var setClauses []string
	for _, col := range table.Columns {
		if col.Name == table.PKColumn {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", col.Name))
	}
	return fmt.Sprintf("UPDATE `%s` SET %s WHERE `%s` = ?",
		table.Name, strings.Join(setClauses, ", "), table.PKColumn)
}

func loadConfig(path string) (DBConfig, error) {
	var config DBConfig
	file, err := os.Open(path)
	if err != nil {
		return config, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	return config, err
}

func connectDB(cfg DBConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(150)
	db.SetMaxIdleConns(75)
	db.SetConnMaxLifetime(5 * time.Minute)
	return db, nil
}

func generateFakeData(table TableDef) ([]interface{}, error) {
	values := make([]interface{}, 0, len(table.Columns))
	for _, col := range table.Columns {
		if col.Name == table.PKColumn {
			continue
		}
		var val interface{}
		ruleProcessed := false
		if col.FakerRule != "" {
			// Contoh parsing untuk aturan INT[min:max]
			if strings.HasPrefix(col.FakerRule, "INT[") {
				re := regexp.MustCompile(`INT\[(\d+):(\d+)\]`)
				matches := re.FindStringSubmatch(col.FakerRule)
				if len(matches) == 3 {
					min, _ := strconv.Atoi(matches[1])
					max, _ := strconv.Atoi(matches[2])
					if max > min {
						val = rand.Intn(max-min+1) + min
						ruleProcessed = true
					}
				}
			}
			// Anda bisa menambahkan parser untuk aturan lain di sini (misal: STRING, ENUM, dll)
		}

		if ruleProcessed {
			values = append(values, val)
			continue // Lanjut ke kolom berikutnya
		}

		colType := strings.ToLower(col.Type)
		colName := strings.ToLower(col.Name)
		switch colType {
		case "varchar", "text", "char":
			var generatedString string
			switch {
			case strings.Contains(colName, "email"):
				generatedString = faker.Email()
			case strings.Contains(colName, "uuid"):
				generatedString = faker.UUIDHyphenated()
			case strings.Contains(colName, "name"):
				generatedString = faker.Name()
			case strings.Contains(colName, "phone"):
				generatedString = faker.Phonenumber()
			case strings.Contains(colName, "address"):
				generatedString = faker.Sentence()
			default:
				generatedString = faker.Sentence()
			}
			if col.Size > 0 {
				runes := []rune(generatedString)
				if len(runes) > col.Size {
					val = string(runes[:col.Size])
				} else {
					val = generatedString
				}
			} else {
				val = generatedString
			}
		case "int", "bigint", "smallint", "mediumint":
			val = rand.Int63n(1000000) + 1
		case "tinyint":
			if col.Size == 1 {
				val = rand.Intn(2)
			} else {
				val = rand.Intn(127)
			}
		case "double":
			val = rand.Float64() * 10000
		case "decimal", "float":
			f, _ := strconv.ParseFloat(faker.AmountWithCurrency(), 64)
			val = f
		case "date":
			val = time.Now()
		case "datetime", "timestamp":
			val = time.Now()
		default:
			val = faker.Word()
		}
		values = append(values, val)
	}
	return values, nil
}
