// todo: add description
// todo: add output format (json, csv, etc.) (default: json)
// todo: add output compression (gzip, bzip2, etc.) (default: gzip)
// todo: add output name template (template) (default: result_%period%_%start%_%end%.json.gz)
// todo: add output name date format (default: 060102_150405)
// todo: add save/load last processed period
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
	outputPath := flag.String("output", ".\\", "directory to save output files (default: current directory)")
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

	if *startStr == "now" {
		*startStr = time.Now().Format("2006-01-02 15:04:05")
	}

	start, err := time.Parse("2006-01-02 15:04:05", *startStr)
	if err != nil {
		log.Fatalf("invalid start datetime: %s", err)
	}

	period, err := time.ParseDuration(*periodStr)
	if err != nil || period > 24*time.Hour {
		log.Fatalf("invalid period: %s", err)
	}

	// check output path and create if not exists
	if _, err := os.Stat(*outputPath); os.IsNotExist(err) {
		err := os.MkdirAll(*outputPath, 0755)
		if err != nil {
			log.Fatalf("invalid output path: %s", err)
		}
	}

	progStart := time.Now()
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
	fmt.Println("Total time: ", time.Since(progStart))
}

type Time time.Time

func (t Time) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", time.Time(t).Format("2006-01-02 15:04:05.000"))), nil
}

func writeOnePeriod(sourceDb *sql.DB, start time.Time, period time.Duration, path string) {
	end := start.Add(period)
	if period < 0 {
		start = start.Add(period)
		end = start.Add(-period)
	}

	startStr := start.Format("2006-01-02 15:04:05")
	endStr := end.Format("2006-01-02 15:04:05")
	fmt.Println(time.Now().Format("2006-01-02 15:04:05"), " : ", startStr, " - ", endStr)

	// qCount := fmt.Sprintf("SELECT count(*) FROM history h WHERE h.[DateTime] BETWEEN '%s' AND '%s' and h.tagname like '%%' and h.Value is not null", startStr, endStr)
	// rowsCount := sourceDb.QueryRow(qCount)
	// var count int
	// err := rowsCount.Scan(&count)
	// if err != nil {
	// 	log.Println(err.Error())
	// }
	// fmt.Print(" %%: ", 0)
	// currRecord := 0

	q := fmt.Sprintf("SELECT h.TagName, h.[DateTime], h.Value FROM history h WHERE h.[DateTime] BETWEEN '%s' AND '%s' and h.tagname like '%%' and h.Value is not null", startStr, endStr)
	rows, err := sourceDb.Query(q)
	if err != nil {
		log.Println(err.Error())
	}
	defer rows.Close()

	file, err := os.Create(path + "/result_" + start.Format("060102_150405") + "_" + end.Format("060102_150405") + ".json.gz")
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
		// currRecord++
		// fmt.Print("\b\b\b\b", "%d3%%", currRecord*100/count)
	}

	if err := encoder.Encode(data); err != nil {
		log.Fatal(err)
	}
}

func writeRow(rows *sql.Rows, data []map[string]interface{}) []map[string]interface{} {
	var tagName string
	var dateTime time.Time
	var value float64

	if err := rows.Scan(&tagName, &dateTime, &value); err != nil {
		log.Println(err.Error())
	}

	d := map[string]interface{}{
		"TagName":  tagName,
		"DateTime": Time(dateTime),
		"Value":    value,
	}

	return append(data, d)
}
