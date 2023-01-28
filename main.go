package main

import (
	// "context"
	"compress/gzip"
	"database/sql"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	// Set end time to current 2023-01-23 12:00:00
	start, _ := time.Parse("2006-01-02 15:04:05", "2023-01-05 20:00:00")

	// Connect to MSSQL source database
	sourceDb, err := sql.Open("mssql", "server=139.158.31.1;port=1433;user id=sa;password=!QAZ1qaz12345;database=runtime;TrustServerCertificate=true;encrypt=disable;connection timeout=3000;")
	if err != nil {
		log.Fatal(err)
	}
	defer sourceDb.Close()

	// Connect to ClickHouse destination database
	destDb, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "runtime",
			Username: "admin",
			Password: "password123",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer destDb.Close()

	// Set context
	// ctx := context.Background()

	// Create a buffer to store data before inserting into ClickHouse
	// var buffer []string
	period := time.Minute * 5
	end := start.Add(period)
	for i := 0; i < 2; i++ {
		// Copy data from MSSQL source to buffer for each hour
		startStr := start.Format("2006-01-02 15:04:05")
		endStr := end.Format("2006-01-02 15:04:05")
		fmt.Println(time.Now().Format("2006-01-02 15:04:05"), " : ", startStr, " - ", endStr)

		q := fmt.Sprintf("SELECT h.TagName, h.[DateTime], h.Value FROM history h WHERE h.[DateTime] BETWEEN '%s' AND '%s' and h.tagname like '%%' and h.Value is not null", startStr, endStr)
		rows, err := sourceDb.Query(q)
		if err != nil {
			log.Println(err.Error())
		}
		defer rows.Close()

		file, err := os.Create("result_" + start.Format("2006_01_02_15_04_05") + ".bin")
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		gzipWriter := gzip.NewWriter(file)
		defer gzipWriter.Close()

		encoder := gob.NewEncoder(gzipWriter)

		// encoder := gob.NewEncoder(file)
		gob.Register(time.Time{})

		// create an empty slice to hold the row data
		var data []map[string]interface{}

		// iterate through the rows
		for rows.Next() {
			// create a map to hold the column data
			columns := make(map[string]interface{})

			// get the column names
			columnNames, err := rows.Columns()
			if err != nil {
				log.Fatal(err)
			}

			// create a slice to hold the column values
			values := make([]interface{}, len(columnNames))

			// create a slice of pointers to the column values
			valuePtrs := make([]interface{}, len(columnNames))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			// scan the row data into the column values
			if err := rows.Scan(valuePtrs...); err != nil {
				log.Fatal(err)
			}

			// add the column data to the map
			for i, columnName := range columnNames {
				columns[columnName] = values[i]
			}

			// add the map to the data slice
			data = append(data, columns)
		}

		//encode data to binary
		if err := encoder.Encode(data); err != nil {
			log.Fatal(err)
		}

		// for rows.Next() {
		// 	var tag string
		// 	var date time.Time
		// 	var value float32
		// 	if err := rows.Scan(&tag, &date, &value); err != nil {
		// 		log.Println(err.Error())
		// 	}

		// 	// Append data to buffer
		// 	buffer = append(buffer, fmt.Sprintf("('%s', '%s', %v)", tag, date.Format("2006-01-02 15:04:05.000"), value))
		// 	// Check if buffer has reached a certain size, then insert data into destination ClickHouse
		// 	if len(buffer) >= 100000 {
		// 		err = destDb.Exec(ctx, "INSERT INTO history (tag_name, date, value) VALUES "+strings.Join(buffer, ","))
		// 		if err != nil {
		// 			log.Println(err.Error())
		// 		}
		// 		buffer = nil
		// 	}
		// }
		start = start.Add(period)
		end = end.Add(period)
	}

	// Insert remaining data in buffer into destination ClickHouse
	// if len(buffer) > 0 {
	// 	destDb.Exec(ctx, "INSERT INTO history (tag_name, date, value) VALUES "+strings.Join(buffer, ","))
	// 	if err != nil {
	// 		log.Println(err.Error())
	// 	}
	// }
}
