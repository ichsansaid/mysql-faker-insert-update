# Go Dynamic Data Simulator & Load Generator 🚀

[](https://golang.org/dl/)

**Go Dynamic Data Simulator** adalah sebuah *command-line tool* performa tinggi yang ditulis dalam Go untuk mengisi dan memberikan beban (load) pada database MySQL. Alat ini secara dinamis membaca skema tabel dari file DDL, menghasilkan data palsu (*fake data*) yang relevan secara cerdas, dan mengeksekusinya secara konkuren untuk mensimulasikan beban kerja aplikasi di dunia nyata.

Fitur utamanya adalah kemampuan untuk mendefinisikan aturan pembuatan data kustom langsung di dalam komentar file DDL, menjadikan skema sebagai satu-satunya sumber kebenaran (*single source of truth*).

-----

## Fitur Utama ✨

  * **Parsing DDL Dinamis**: Tidak perlu *hardcode* skema. Cukup sediakan file `CREATE TABLE` dan aplikasi akan beradaptasi secara otomatis.
  * **Worker Konkuren**: Menggunakan goroutine untuk menghasilkan *throughput* yang tinggi saat memasukkan data ke database.
  * **Generasi Data Cerdas**: Memanfaatkan `go-faker` untuk membuat data yang masuk akal berdasarkan nama dan tipe data kolom.
  * **Aturan Kustom Langsung di DDL**: Tentukan aturan pembuatan data yang spesifik (misalnya, rentang angka) langsung di dalam komentar DDL.
  * **Mode Operasi Fleksibel**: Mendukung mode `insert` (hanya INSERT) dan `mixed` (kombinasi INSERT dan UPDATE).
  * **Laporan Real-time**: Memantau total operasi dan *Queries Per Second* (QPS) langsung di terminal saat aplikasi berjalan.
  * **Konfigurasi Eksternal**: Pengaturan koneksi database yang aman melalui file `config.json`.

-----

## Cara Kerja

1.  **Inisialisasi**: Aplikasi membaca argumen dari *command-line*, file `config.json` untuk koneksi database, dan file `schema.sql` untuk struktur tabel.
2.  **Parsing Skema**: Struktur tabel, termasuk nama kolom, tipe data, dan aturan kustom dari komentar, di-parse dari file DDL.
3.  **Pembuatan Tugas**: *Main thread* menghasilkan tugas (`INSERT` atau `UPDATE`) dan mengirimkannya melalui sebuah *channel*.
4.  **Eksekusi oleh Worker**: Sekumpulan *worker* (goroutine) menerima tugas dari *channel* dan mengeksekusinya ke database menggunakan *prepared statements* untuk performa maksimal.
5.  **Pelaporan**: Sebuah goroutine terpisah secara periodik melaporkan kemajuan dan QPS ke konsol.

-----

## Prasyarat

  * [Go](https://golang.org/dl/) versi 1.18 atau lebih baru.
  * Database [MySQL](https://www.mysql.com/) yang sedang berjalan.

-----

## Instalasi & Konfigurasi

1.  **Clone repositori ini:**

    ```bash
    git clone <url-repositori-anda>
    cd <nama-direktori>
    ```

2.  **Buat file konfigurasi `config.json`:**

    ```json
    {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "password": "your_password",
        "dbname": "testdb"
    }
    ```

3.  **Buat file skema DDL `schema.sql`:**
    Ini adalah tempat Anda mendefinisikan struktur tabel. Anda juga bisa menambahkan aturan kustom di sini.

    ```sql
    CREATE TABLE `EVENT_RECORDS` (
        `EVENT_DATE` date NOT NULL,
        `PRIMARY_IDENTIFIER` varchar(24) NOT NULL /*TYPE: INT[10000000:99999999]*/,
        `SECONDARY_IDENTIFIER` varchar(12) NOT NULL,
        `EVENT_TIME` int NOT NULL,
        `REFERENCE_CODE` varchar(24) NOT NULL,
        `EVENT_TYPE` varchar(24) NOT NULL,
        `AMOUNT` double DEFAULT NULL,
        `CHANNEL_CODE` varchar(24) DEFAULT NULL /*TYPE: INT[1:100]*/,
        `INGESTION_TIMESTAMP` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `PROCESSING_TIMESTAMP` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `SOURCE_PARTITION` int DEFAULT NULL,
        `SOURCE_OFFSET` bigint DEFAULT NULL,
        PRIMARY KEY (`EVENT_DATE`, `PRIMARY_IDENTIFIER`, `SECONDARY_IDENTIFIER`, `EVENT_TIME`, `REFERENCE_CODE`, `EVENT_TYPE`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
    ```

4.  **Build aplikasi:**

    ```bash
    go build .
    ```

-----

## Penggunaan

Jalankan aplikasi dari terminal dengan menyediakan flag yang diperlukan.

### Sintaks

```bash
./<nama-binary> -ops=<jumlah> -threads=<jumlah> -config="config.json" -ddl="schema.sql" [opsi-lain]
```

### Opsi (Flags)

| Flag | Deskripsi | Default |
| :--- | :--- | :--- |
| `-ops` | **Wajib.** Jumlah total operasi (`INSERT`/`UPDATE`) yang akan dilakukan. | `10000` |
| `-threads`| Jumlah *worker* (goroutine) yang akan dijalankan secara konkuren. | `10` |
| `-config` | **Wajib.** Path menuju file konfigurasi database `config.json`. | `""` |
| `-ddl` | **Wajib.** Path menuju file skema tabel `.sql`. | `""` |
| `-mode` | Mode operasi. Pilihan: `insert` atau `mixed`. | `insert` |
| `-update-ratio` | Rasio operasi `UPDATE` dalam mode `mixed` (misal: 0.3 untuk 30%). | `0.3` |

### Contoh

  * **Menjalankan 50,000 `INSERT` dengan 20 threads:**

    ```bash
    ./go-datagen -ops=50000 -threads=20 -config="config.json" -ddl="schema.sql" -mode="insert"
    ```

  * **Menjalankan 1,000,000 operasi campuran (70% INSERT, 30% UPDATE) dengan 100 threads:**

    ```bash
    ./go-datagen -ops=1000000 -threads=100 -config="config.json" -ddl="user_schema.sql" -mode="mixed" -update-ratio=0.3
    ```

-----

## Aturan Faker Kustom di DDL

Anda bisa mengontrol data yang dihasilkan dengan menambahkan komentar berformat khusus pada DDL Anda.

**Sintaks Saat Ini:**

  * `/*TYPE: INT[min:max]*/`: Menghasilkan angka integer acak dalam rentang `min` dan `max` (inklusif).

**Contoh:**
Kolom `MID_MPAN` akan diisi dengan angka acak antara `10000000` dan `99999999`.

```sql
`MID_MPAN` varchar(24) NOT NULL /*TYPE: INT[10000000:99999999]*/,
```

Fitur ini dapat diperluas dengan mudah di dalam fungsi `generateFakeData` untuk mendukung lebih banyak aturan.

-----

## ⚠️ Catatan Penting

Mode `mixed` (yang mencakup operasi `UPDATE`) **memerlukan sebuah kolom Primary Key tunggal dengan `AUTO_INCREMENT`** pada tabel target. Aplikasi menggunakan `LastInsertId` untuk melacak baris mana yang bisa di-update. Jika tidak ada PK `AUTO_INCREMENT`, aplikasi akan secara otomatis beralih ke mode `insert` saja.

-----

## Lisensi

Proyek ini dilisensikan di bawah [MIT License](https://www.google.com/search?q=LICENSE).