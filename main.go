// todo: refactor code
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

	// "encoding/csv"
	"encoding/json"
	"flag"
	"fmt"

	// "log"
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
	// format := flag.String("format", "json", "output format (json, csv, etc.)")

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
	allPeriodStr := *periodStr

	if *startStr == "last" {
		last = true
		// read last processed period from file
		file, err := os.Open(*lastFile)
		if err != nil {
			// log.Fatalf("error opening last file: %s", err)
			fmt.Fprintln(os.Stderr, "error opening last file: ", err)
		}
		defer file.Close()
		jsonParser := json.NewDecoder(file)
		var lastDateStr map[string]string
		if err = jsonParser.Decode(&lastDateStr); err != nil {
			// log.Fatalf("error parsing last file: %s", err)
			fmt.Fprintln(os.Stderr, "error parsing last file: ", err)
		}
		*startStr = lastDateStr["end"]
		// fmt.Println("startStr: ", *startStr)
		// fix: костыль
		t, err := time.Parse("2006-01-02 15:04:05", *startStr)
		t = t.Add(-time.Hour * 6)
		if err != nil {
			// log.Fatalf("error parsing last file: %s", err)
			fmt.Fprintln(os.Stderr, "error parsing last file: ", err)

		}
		allPeriodStr = time.Since(t).String()
	}

	start, err := time.Parse("2006-01-02 15:04:05", *startStr)
	if err != nil {
		// log.Fatalf("invalid start datetime: %s", err)
		fmt.Fprintln(os.Stderr, "invalid start datetime: ", err)
	}

	// If no period is specified, or if the count is less than 1, then print an error message and exit
	if *periodStr == "" || *count < 1 {
		fmt.Println("Usage of ", os.Args[0], ":")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *periodStr == "" || *count < 1 {
		fmt.Println("Usage of ", os.Args[0], ":")
		flag.PrintDefaults()
		os.Exit(1)
	}

	period, err := time.ParseDuration(*periodStr)
	if err != nil {
		// log.Fatalf("invalid period: %s", err)
		fmt.Fprintln(os.Stderr, "invalid period: ", err)
	}

	allPeriod, err := time.ParseDuration(allPeriodStr)
	if err != nil {
		// log.Fatalf("invalid period: %s", err)
		fmt.Fprintln(os.Stderr, "invalid period: ", err)
	}

	if allPeriod > period {
		*count = int(allPeriod / period)
	}

	if period > time.Hour*24 {
		*count = int(period / (time.Hour * 24))
		period = time.Hour * 24
	}

	fmt.Println("count: ", *count)
	// log.Printf("start: %s, period: %s, count: %d", start, period, *count)

	// check output path and create if not exists
	if _, err := os.Stat(*outputPath); os.IsNotExist(err) {
		err := os.MkdirAll(*outputPath, 0755)
		if err != nil {
			// log.Fatalf("invalid output path: %s", err)
			fmt.Fprintln(os.Stderr, "invalid output path: ", err)
		}
	}

	progStart := time.Now()
	// Connect to MSSQL source database
	sourceDb, err := sql.Open("mssql", "server=139.158.31.1;port=1433;user id=sa;password=!QAZ1qaz12345;database=runtime;TrustServerCertificate=true;encrypt=disable;connection timeout=3000;")
	if err != nil {
		// log.Fatal(err)
		fmt.Fprintln(os.Stderr, err)
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
			// log.Fatalf("error creating last file: %s", err)
			fmt.Fprintln(os.Stderr, "error creating last file: ", err)
		}
		defer file.Close()
		jsonEncoder := json.NewEncoder(file)
		if err = jsonEncoder.Encode(lastDateStr); err != nil {
			// log.Fatalf("error encoding last file: %s", err)
			fmt.Fprintln(os.Stderr, "error encoding last file: ", err)
		}
	}
	fmt.Println("Total time: ", time.Since(progStart))
	// log.Println("Total time: ", time.Since(progStart))
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
	// log.Println(time.Now().Format("2006-01-02 15:04:05"), " : ", startStr, " - ", endStr)

	q := fmt.Sprintf("SELECT h.TagName, h.[DateTime], h.Value FROM history h WHERE h.[DateTime] BETWEEN '%s' AND '%s' and h.tagname like '%%' and h.Value is not null", startStr, endStr)
	rows, err := sourceDb.Query(q)
	if err != nil {
		// log.Println(err.Error())
		fmt.Fprintln(os.Stderr, err.Error())
	}
	defer rows.Close()

	file, err := os.Create(path + "\\result_" + start.Format("060102_150405") + "_" + end.Format("060102_150405") + ".json.gz")
	if err != nil {
		// log.Fatal(err)
		fmt.Fprintln(os.Stderr, err)
	}
	defer file.Close()

	gzipWriter := gzip.NewWriter(file)
	defer gzipWriter.Close()

	encoder := json.NewEncoder(gzipWriter)
	// if outFormat == "csv" {
	// 	encoder = csv.NewEncoder(gzipWriter)
	// }

	var data []map[string]interface{}

	for rows.Next() {
		data = writeRow(rows, data)
		// currRecord++
		// fmt.Print("\b\b\b\b", "%d3%%", currRecord*100/count)
	}

	if err := encoder.Encode(data); err != nil {
		// log.Fatal(err)
		fmt.Fprintln(os.Stderr, err)
	}
}

func writeRow(rows *sql.Rows, data []map[string]interface{}) []map[string]interface{} {
	var tagName string
	var dateTime time.Time
	var value float64

	if err := rows.Scan(&tagName, &dateTime, &value); err != nil {
		// log.Println(err.Error())
		fmt.Fprintln(os.Stderr, err.Error())
	}

	d := map[string]interface{}{
		"TagName":  tagName,
		"DateTime": Time(dateTime),
		"Value":    value,
	}

	return append(data, d)
}
