package main

import (
	"archive/zip"
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	startStr := flag.String("start", "", "start datetime (format: '2006-01-02 15:04:05')")
	periodStr := flag.String("period", "", "duration of a period (format: 1h, 5m, ...) (max: 24h)")
	outputPath := flag.String("output", ".\\", "directory to save output files (default: current directory)")
	count := flag.Int("count", 1, "number of periods to process (default: 1)")
	fileFormat := flag.String("format", "json", "output file format (default: json)")
	compression := flag.String("compression", "gzip", "output file compression (default: gzip)")
	template := flag.String("template", "result_%period%_%start%_%end%."+*fileFormat+"."+*compression, "output name template (default: result_%period%_%start%_%end%."+*fileFormat+"."+*compression+")")
	dateFormat := flag.String("date-format", "060102_150405", "output name date format (default: 060102_150405)")
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

	progStart := time.Now()
	// Connect to MSSQL source database
	sourceDb, err := sql.Open("mssql", "server=139.158.31.1;port=1433;user id=sa;password=!QAZ1qaz12345;database=runtime;TrustServerCertificate=true;encrypt=disable;connection timeout=3000;")
	if err != nil {
		log.Fatalf("failed to connect to source database: %s", err)
	}
	defer sourceDb.Close()
	err = sourceDb.Ping()
	if err != nil {
		log.Fatalf("failed to ping source database: %s", err)
	}

	for i := 0; i < *count; i++ {
		writeOnePeriod(sourceDb, start, period, *outputPath, fileFormat, compression, template, dateFormat, outputPath, nil)
		start = start.Add(period)
	}
	fmt.Println("Total time: ", time.Since(progStart))
}

type Time time.Time

func (t Time) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", time.Time(t).Format("2006-01-02 15:04:05.000"))), nil
}

func (t Time) MarshalText() ([]byte, error) {
	return []byte(time.Time(t).Format("2006-01-02 15:04:05.000")), nil
}

func (t Time) MarshalXMLAttr(name xml.Name) (xml.Attr, error) {
	return xml.Attr{Name: name, Value: time.Time(t).Format("2006-01-02 15:04:05.000")}, nil
}

func (t Time) String() string {
	return time.Time(t).Format("2006-01-02 15:04:05.000")
}

func writeOnePeriod(sourceDb *sql.DB, start time.Time, period time.Duration, path string, fileFormat *string, compression *string, template *string, dateFormat *string, outputPath *string, result *[]map[string]interface{}) {
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

	// format file name with template and date format
	fileName := *template
	fileName = strings.Replace(fileName, "%period%", period.String(), -1)
	fileName = strings.Replace(fileName, "%start%", start.Format(*dateFormat), -1)
	fileName = strings.Replace(fileName, "%end%", end.Format(*dateFormat), -1)

	// create output file
	file, err := os.Create(*outputPath + "\\" + fileName)
	if err != nil {
		log.Fatalf("failed to create output file: %s", err)
	}
	defer file.Close()

	var data []map[string]interface{}

	for rows.Next() {
		data = writeRow(rows, data)
	}

	if *fileFormat == "json" {
		// write data to json file
		enc := json.NewEncoder(file)
		enc.SetIndent("", "  ")
		err = enc.Encode(data)
		if err != nil {
			log.Fatalf("failed to write json file: %s", err)
		}
	}

	if *fileFormat == "csv" {
		// write data to csv file
		w := csv.NewWriter(file)
		for _, d := range data {
			record := []string{d["TagName"].(string), d["DateTime"].(Time).String(), fmt.Sprintf("%f", d["Value"].(float64))}
			if err := w.Write(record); err != nil {
				log.Fatalf("failed to write csv file: %s", err)
			}
		}
		w.Flush()
	}

	if *fileFormat == "xml" {
		// write data to xml file
		enc := xml.NewEncoder(file)
		enc.Indent("", "  ")
		err = enc.Encode(data)
		if err != nil {
			log.Fatalf("failed to write xml file: %s", err)
		}
	}

	if *compression == "gzip" {
		// compress file
		gzipFile(file)
	}

	if *compression == "zip" {
		// compress file
		zipFile(file)
	}

	if result != nil {
		*result = data
	}
}

func gzipFile(file *os.File) {
	// close file before compress
	file.Close()

	// compress file
	gzipFile, err := os.Create(file.Name() + ".gz")
	if err != nil {
		log.Fatalf("failed to create gzip file: %s", err)
	}
	defer gzipFile.Close()

	gzipWriter := gzip.NewWriter(gzipFile)
	defer gzipWriter.Close()

	file, err = os.Open(file.Name())
	if err != nil {
		log.Fatalf("failed to open file: %s", err)
	}
	defer file.Close()

	_, err = io.Copy(gzipWriter, file)
	if err != nil {
		log.Fatalf("failed to copy file: %s", err)
	}
}

func zipFile(file *os.File) {
	// close file before compress
	file.Close()

	// compress file
	zipFile, err := os.Create(file.Name() + ".zip")
	if err != nil {
		log.Fatalf("failed to create zip file: %s", err)
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	file, err = os.Open(file.Name())
	if err != nil {
		log.Fatalf("failed to open file: %s", err)
	}
	defer file.Close()

	f, err := zipWriter.Create(file.Name())
	if err != nil {
		log.Fatalf("failed to create file in zip: %s", err)
	}

	_, err = io.Copy(f, file)
	if err != nil {
		log.Fatalf("failed to copy file: %s", err)
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
