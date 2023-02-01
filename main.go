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
	startStr := flag.String("start", "", "start datetime (format: '2006-01-02 15:04:05' or 'now' or 'yesterday' or 'today' or 'last')")
	periodStr := flag.String("period", "", "duration of a period (format: 1h, 5m, ...) (greater than 24h will be split to 24h periods)")
	outputPath := flag.String("output", ".\\", "directory to save output files")
	count := flag.Int("count", 1, "number of periods to process")
	lastFile := flag.String("last", "mssql2file.last", "file to save/load last processed period")
	help := flag.Bool("help", false, "show help")

	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	if *startStr == "" {
		fmt.Println("Usage of ", os.Args[0], ":")
		flag.PrintDefaults()
		fmt.Println("--- for any questions please contact: Novikov A.V. (better not) ---")
		os.Exit(1)
	}

	last := false

	switch *startStr {
	case "now":
		*startStr = time.Now().Format("2006-01-02 15:04:05")
	case "yesterday":
		*startStr = time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05")
		*periodStr = "24h"
	case "today":
		*startStr = time.Now().Format("2006-01-02 15:04:05")
		*periodStr = "24h"
	case "last":
		last = true
		// read last processed period from file
		file, err := os.Open(*lastFile)
		if err != nil {
			log.Fatalf("error opening last file: %s", err)
		}
		defer file.Close()
		jsonParser := json.NewDecoder(file)
		var lastDateStr map[string]string
		if err = jsonParser.Decode(&lastDateStr); err != nil {
			log.Fatalf("error parsing last file: %s", err)
		}
		*startStr = lastDateStr["end"]
		// fmt.Println("startStr: ", *startStr)
		// костыль
		t, err := time.Parse("2006-01-02 15:04:05", *startStr)
		t = t.Add(-time.Hour * 6)
		if err != nil {
			log.Fatalf("error parsing last file: %s", err)
		}
		*periodStr = time.Since(t).String()
		// fmt.Println("t: ", t)
		// fmt.Println("now utc", time.Now().UTC())
		// fmt.Println("now local ", time.Now().Local())
		// fmt.Println("now ", time.Now())
		// fmt.Println("period: ", *periodStr)
	}

	if *startStr == "now" {
		*startStr = time.Now().Format("2006-01-02 15:04:05")
	}

	start, err := time.Parse("2006-01-02 15:04:05", *startStr)
	if err != nil {
		log.Fatalf("invalid start datetime: %s", err)
	}

	if *periodStr == "" || *count < 1 {
		fmt.Println("Usage of ", os.Args[0], ":")
		flag.PrintDefaults()
		os.Exit(1)
	}

	period, err := time.ParseDuration(*periodStr)
	if err != nil {
		log.Fatalf("invalid period: %s", err)
	}

	if period > 24*time.Hour {
		// split period to 24h periods and add count
		*count += int(period / (24 * time.Hour))
		period = 24 * time.Hour
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

	if last {
		// save last processed period to file
		lastDateStr := map[string]string{
			// "start": start.Add(-period).Format("2006-01-02 15:04:05"),
			"end": start.Format("2006-01-02 15:04:05"),
		}
		file, err := os.Create(*lastFile)
		if err != nil {
			log.Fatalf("error creating last file: %s", err)
		}
		defer file.Close()
		jsonEncoder := json.NewEncoder(file)
		if err = jsonEncoder.Encode(lastDateStr); err != nil {
			log.Fatalf("error encoding last file: %s", err)
		}
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
