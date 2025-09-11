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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-faker/faker/v4"
	_ "github.com/go-sql-driver/mysql"
	"github.com/shirou/gopsutil/v3/cpu"
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
	RowCount  int
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

type RequestResult struct {
	StartExec time.Time
	EndExec   time.Time
}

type WorkerResult struct {
	WorkerID     int
	OpsCompleted int64
	TotalLatency time.Duration
	MaxLatency   time.Duration
	MinLatency   time.Duration
	Requests     []RequestResult
}

const (
	OpInsert = "INSERT"
	OpUpdate = "UPDATE"
)

func main() {

	threadCount := flag.Int("threads", 10, "Number of GoRoutines to spawn")
	totalOps := flag.Int("ops", 10000, "Total number of operations (rows) to perform")
	ddlPath := flag.String("ddl", "", "Path to the DDL (.sql) file")
	configPath := flag.String("config", "", "Path to the database config (.json) file")
	mode := flag.String("mode", "insert", "Operation mode: 'insert' or 'mixed'")
	updateRatio := flag.Float64("update-ratio", 0.3, "Ratio of UPDATEs in 'mixed' mode (e.g., 0.3 for 30%)")
	batchInsertSize := flag.Int("batch-insert", 1, "Number of rows to insert in a single batch statement")
	flag.Parse()

	if *ddlPath == "" || *configPath == "" {
		fmt.Println("Error: Both -ddl and -config arguments are required.")
		flag.Usage()
		os.Exit(1)
	}
	if *mode != "insert" && *mode != "mixed" {
		log.Fatalf("Error: Invalid mode '%s'. Alowed modes are 'insert' or 'mixed'.", *mode)
	}
	if *batchInsertSize < 1 {
		log.Println("Warning: batch-insert size must be at least 1. Defaulting to 1.")
		*batchInsertSize = 1
	}

	log.Println("Starting data simulator...")

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

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
	log.Printf("Mode: %s | Worker threads: %d | Total operations: %d | Batch Insert Size: %d", *mode, *threadCount, *totalOps, *batchInsertSize)

	var workerWg sync.WaitGroup
	jobChannel := make(chan SQLJob, *threadCount*2)
	idTracker := &IDTracker{}
	resultsChan := make(chan *WorkerResult, *threadCount)
	workers := make([]WorkerResult, *threadCount)

	for i := 0; i < *threadCount; i++ {
		workerWg.Add(1)
		go worker(i+1, &workers[i], &workerWg, cfg, jobChannel, resultsChan, idTracker, table)
	}

	startDate := time.Now()

	doneReporter := make(chan bool)
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		var m runtime.MemStats

		var beforePendingOps int64 = 0
		var beforeCompletedOps int64 = 0
		var lastCheckTime time.Time = time.Now()

		for {
			select {
			case <-ticker.C:

				var totalCompletedOps int64 = 0
				var pendingRequests int = 0
				var totalLatency time.Duration = 0
				var maxLatency time.Duration = 0
				var oldestPendingStart time.Time

				for i := range workers {
					for j := range workers[i].Requests {
						request := workers[i].Requests[j]
						if request.EndExec.IsZero() {
							pendingRequests++

							if oldestPendingStart.IsZero() || request.StartExec.Before(oldestPendingStart) {
								oldestPendingStart = request.StartExec
							}
						} else {

							totalCompletedOps++
							requestLatency := request.EndExec.Sub(request.StartExec)
							totalLatency += requestLatency
							if requestLatency > maxLatency {
								maxLatency = requestLatency
							}
						}
					}
				}

				beforeCompletedOps = totalCompletedOps - beforeCompletedOps
				beforePendingOps = int64(pendingRequests) - int64(beforePendingOps)
				if beforePendingOps < 0 {
					beforePendingOps = 0
				}

				secondsSinceLastCheck := time.Since(lastCheckTime).Seconds()

				var ageOfOldestPending float64
				if !oldestPendingStart.IsZero() {
					ageOfOldestPending = time.Since(oldestPendingStart).Seconds()
				}

				var qpsInstan float64
				if secondsSinceLastCheck > 0 {
					qpsInstan = float64(beforeCompletedOps) / secondsSinceLastCheck
				}

				var avgLatencyMs float64
				if totalCompletedOps > 0 {

					avgLatencyMs = float64(totalLatency.Milliseconds()) / float64(totalCompletedOps)
				}

				percent := (float64(totalCompletedOps) / float64(*totalOps)) * 100

				cpuPercentages, _ := cpu.Percent(0, false)
				cpuUsage := cpuPercentages[0]
				runtime.ReadMemStats(&m)
				memAllocMB := m.Alloc / 1024 / 1024

				progressMsg := fmt.Sprintf(
					"Progress: %d/%d (%.2f%%) | Done: +%d | Total Pending: %d | Oldest: %.1fs | QPS: %.2f | Latency(avg/max): %.2fms/%.0fms | CPU: %.1f%% | Mem: %dMB",
					totalCompletedOps,
					*totalOps,
					percent,
					beforeCompletedOps,
					pendingRequests,
					ageOfOldestPending,
					qpsInstan,
					avgLatencyMs,
					float64(maxLatency.Milliseconds()),
					cpuUsage,
					memAllocMB,
				)

				beforeCompletedOps = totalCompletedOps
				lastCheckTime = time.Now()
				fmt.Println(progressMsg)

			case <-doneReporter:
				return
			}
		}
	}()

	go func() {
		defer close(jobChannel)
		var insertBatchData []interface{}
		var batchRowCount int = 0
		flushBatch := func() {
			if batchRowCount > 0 {
				job := SQLJob{Operation: OpInsert, Data: insertBatchData, RowCount: batchRowCount}
				jobChannel <- job
				insertBatchData = nil
				batchRowCount = 0
			}
		}
		for i := 0; i < *totalOps; i++ {
			var job SQLJob
			isUpdateOp := (table.PKColumn != "" && *mode == "mixed" && rand.Float64() < *updateRatio)
			if isUpdateOp {
				flushBatch()
				if targetID, ok := idTracker.GetRandomID(); ok {
					record, _ := generateFakeData(table)
					job = SQLJob{Operation: OpUpdate, Data: record, TargetID: targetID, RowCount: 1}
					jobChannel <- job
				} else {
					isUpdateOp = false
				}
			}
			if !isUpdateOp {
				record, _ := generateFakeData(table)
				insertBatchData = append(insertBatchData, record...)
				batchRowCount++
				if batchRowCount == *batchInsertSize {
					flushBatch()
				}
			}
		}
		flushBatch()
	}()

	workerWg.Wait()
	close(resultsChan)
	endDate := time.Now()

	doneReporter <- true
	fmt.Println()

	var finalTotalOps int64
	var maxLatency time.Duration = 0

	log.Println("--- Aggregating results from workers ---")
	for result := range resultsChan {
		finalTotalOps += result.OpsCompleted
		if result.MaxLatency > maxLatency {
			maxLatency = result.MaxLatency
		}
	}

	elapsedTime := endDate.Sub(startDate)
	log.Println("--- Final Report ---")
	log.Printf("A total of %d operations were completed.", finalTotalOps)
	log.Printf("Total time taken: %s", elapsedTime)
	if finalTotalOps > 0 {
		log.Printf("Average QPS (Queries Per Second): %.2f", float64(finalTotalOps)/elapsedTime.Seconds())
		log.Printf("Average time per operation: %.2fms", float64(elapsedTime.Milliseconds())/float64(finalTotalOps))
	}
	log.Printf("Max Latency observed across all workers: %s", maxLatency)
	log.Println("All workers finished. Process complete.")
}

func worker(id int, worker *WorkerResult, wg *sync.WaitGroup, cfg DBConfig, jobChannel <-chan SQLJob, resultsChan chan<- *WorkerResult, idTracker *IDTracker, table TableDef) {
	defer wg.Done()

	db, err := connectDB(cfg)
	if err != nil {
		log.Printf("Worker %d | Failed to connect to database: %v", id, err)
		return
	}
	defer db.Close()

	var updateStmt *sql.Stmt
	if table.PKColumn != "" {
		updateStmt, err = db.Prepare(buildUpdateStatement(table))
		if err != nil {
			log.Printf("Worker %d | Failed to prepare update statement: %v", id, err)
			return
		}
		defer updateStmt.Close()
	}

	var localOpsCount int64 = 0
	var totalLatency time.Duration = 0
	var maxLatency time.Duration = 0
	var minLatency time.Duration = -1

	for job := range jobChannel {
		var execErr error

		var latency time.Duration
		switch job.Operation {
		case OpInsert:
			var res sql.Result
			insertSQL := buildInsertStatement(table, job.RowCount)
			start := time.Now()
			worker.Requests = append(worker.Requests, RequestResult{StartExec: start})
			res, execErr = db.Exec(insertSQL, job.Data...)
			timeEnd := time.Now()
			latency = timeEnd.Sub(start)
			if execErr == nil && table.PKColumn != "" {
				newID, _ := res.LastInsertId()
				idTracker.Add(newID)
			} else if execErr != nil {
				log.Printf("Worker %d | Error: %v", id, execErr)
			}
			worker.Requests[len(worker.Requests)-1].EndExec = timeEnd
		case OpUpdate:
			start := time.Now()
			args := append(job.Data, job.TargetID)
			_, execErr = updateStmt.Exec(args...)
			latency = time.Since(start)

		}
		if execErr != nil {
			log.Printf("Worker %d | Error: %v", id, execErr)
		} else {
			localOpsCount += int64(job.RowCount)
			totalLatency += latency
			if latency > maxLatency {
				maxLatency = latency
			}
			if minLatency == -1 || latency < minLatency {
				minLatency = latency
			}
		}
	}
	worker.WorkerID = id
	worker.OpsCompleted = localOpsCount
	worker.TotalLatency = totalLatency
	worker.MaxLatency = maxLatency
	worker.MinLatency = minLatency

	resultsChan <- worker
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

func buildInsertStatement(table TableDef, batchSize int) string {
	var columnNames []string
	var valuePlaceholders []string

	for _, col := range table.Columns {
		if col.Name == table.PKColumn {
			continue
		}
		columnNames = append(columnNames, fmt.Sprintf("`%s`", col.Name))
	}

	singleRowPlaceholder := fmt.Sprintf("(%s)", strings.Join(strings.Split(strings.Repeat("?", len(columnNames)), ""), ", "))

	for i := 0; i < batchSize; i++ {
		valuePlaceholders = append(valuePlaceholders, singleRowPlaceholder)
	}

	return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
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
		}

		if ruleProcessed {
			values = append(values, val)
			continue
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
		case "date", "datetime", "timestamp":
			val = time.Now()
		default:
			val = faker.Word()
		}
		values = append(values, val)
	}
	return values, nil
}
