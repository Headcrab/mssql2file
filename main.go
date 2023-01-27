package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	// Set end time to current 2023-01-23 12:00:00
	start, _ := time.Parse("2006-01-02 15:04:05", "2022-01-21 11:00:00")

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
			Database: "default",
			Username: "admin",
			Password: "password123",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer destDb.Close()

	// Set context
	ctx := context.Background()

	// Create a buffer to store data before inserting into ClickHouse
	var buffer []string

	period := time.Hour
	end := start.Add(period)
	for i := 0; i < 600; i++ {
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

		for rows.Next() {
			var tag string
			var date time.Time
			var value float32
			if err := rows.Scan(&tag, &date, &value); err != nil {
				log.Println(err.Error())
			}

			// Append data to buffer
			buffer = append(buffer, fmt.Sprintf("('%s', '%s', %v)", tag, date.Format("2006-01-02 15:04:05.000"), value))
			// Check if buffer has reached a certain size, then insert data into destination ClickHouse
			if len(buffer) >= 1000 {
				err = destDb.Exec(ctx, "INSERT INTO history (tag_name, date, value) VALUES "+strings.Join(buffer, ","))
				if err != nil {
					log.Println(err.Error())
				}
				buffer = nil
			}
		}
		start = start.Add(period)
		end = end.Add(period)
	}

	// Insert remaining data in buffer into destination ClickHouse
	if len(buffer) > 0 {
		destDb.Exec(ctx, "INSERT INTO history (tag_name, date, value) VALUES "+strings.Join(buffer, ","))
		if err != nil {
			log.Println(err.Error())
		}
	}
}
