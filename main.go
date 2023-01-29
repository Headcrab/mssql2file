package main

import (
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	startStr := flag.String("start", "", "start datetime (format: '2006-01-02 15:04:05')")
	periodStr := flag.String("period", "", "duration of a period (format: 1h, 5m, ...) (max: 24h)")
	outputPath := flag.String("output", "\\", "directory to save output files (default: current directory)")
	count := flag.Int("count", 1, "number of periods to process (default: 1)")
	help := flag.Bool("help", false, "show help")

	flag.Parse()

	if *startStr == "" || *periodStr == "" || *outputPath == "" || *count < 1 {
		fmt.Println("Usage of ", os.Args[0], ":")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	start, err := time.Parse("2006-01-02 15:04:05", *startStr)
	if err != nil {
		log.Fatalf("invalid start datetime: %s", err)
	}

	period, err := time.ParseDuration(*periodStr)
	if err != nil || period <= 0 || period > 24*time.Hour {
		log.Fatalf("invalid period: %s", err)
	}

	// check output path and create if not exists
	if _, err := os.Stat(*outputPath); os.IsNotExist(err) {
		err := os.MkdirAll(*outputPath, 0755)
		if err != nil {
			log.Fatalf("invalid output path: %s", err)
		}
	}

	// Connect to MSSQL source database
	sourceDb, err := sql.Open("mssql", "server=139.158.31.1;port=1433;user id=sa;password=!QAZ1qaz12345;database=runtime;TrustServerCertificate=true;encrypt=disable;connection timeout=3000;")
	if err != nil {
		log.Fatal(err)
	}
	defer sourceDb.Close()

	for i := 0; i < *count; i++ {
		writeOnePeriod(sourceDb, start, period, *outputPath)
		start = start.Add(period)
	}
}

type Time time.Time

func (t Time) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", time.Time(t).Format("2006-01-02 15:04:05.000"))), nil
}

func writeOnePeriod(sourceDb *sql.DB, start time.Time, period time.Duration, path string) {
	end := start.Add(period)
	startStr := start.Format("2006-01-02 15:04:05")
	endStr := end.Format("2006-01-02 15:04:05")
	fmt.Println(time.Now().Format("2006-01-02 15:04:05"), " : ", startStr, " - ", endStr)

	q := fmt.Sprintf("SELECT h.TagName, h.[DateTime], h.Value FROM history h WHERE h.[DateTime] BETWEEN '%s' AND '%s' and h.tagname like '%%' and h.Value is not null", startStr, endStr)
	rows, err := sourceDb.Query(q)
	if err != nil {
		log.Println(err.Error())
	}
	defer rows.Close()

	file, err := os.Create("to_diode/result_" + start.Format("060102_150405") + "_" + end.Format("060102_150405") + ".json.gz")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	gzipWriter := gzip.NewWriter(file)
	defer gzipWriter.Close()

	encoder := json.NewEncoder(gzipWriter)

	var data []map[string]interface{}

	for rows.Next() {
		data = writeRow(rows, data)
	}

	if err := encoder.Encode(data); err != nil {
		log.Fatal(err)
	}
}

func writeRow(rows *sql.Rows, data []map[string]interface{}) []map[string]interface{} {

	columns := make(map[string]interface{})
	columns["DateTime"] = Time{}

	columnNames, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}

	values := make([]interface{}, len(columnNames))

	valuePtrs := make([]interface{}, len(columnNames))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		// if err := rows.Scan(values...); err != nil {
		log.Fatal(err)
	}

	for i, columnName := range columnNames {
		if columnName == "DateTime" {
			columns[columnName] = values[i].(time.Time).Format("2006-01-02 15:04:05.000")
		} else {
			columns[columnName] = values[i]
		}
	}

	data = append(data, columns)
	return data
}
