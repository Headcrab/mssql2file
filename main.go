package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	// Set end time to current 2023-01-23 12:00:00
	end, _ := time.Parse("2006-01-02 15:04:05", "2023-01-23 20:00:00")

	// Connect to MSSQL source database
	sourceDb, err := sql.Open("mssql", "server=139.158.31.1;port=1433;user id=sa;password=!QAZ1qaz12345;database=runtime;TrustServerCertificate=true;encrypt=disable;connection timeout=3000;")
	if err != nil {
		log.Fatal(err)
	}
	defer sourceDb.Close()

	// Connect to ClickHouse destination database
	// destDb, err := clickhouse.OpenDirect("tcp://127.0.0.1:9000?debug=true")
	destDb, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "password123",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer destDb.Close()

	// Set context
	ctx := context.Background()

	period := -time.Minute * 5
	start := end.Add(period)
	for i := 0; i < 8760; i++ {
		// Copy data from MSSQL source to ClickHouse destination for each hour
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

			// Insert data into destination ClickHouse
			destDb.Exec(ctx, "INSERT INTO history (tag_name, date, value) VALUES (?, ?, ?)", tag, date, value)
			//  Exec("INSERT INTO history (tag_name, date, value) VALUES (?, ?, ?)")
			//  Query("INSERT INTO history (tag_name, date, value) VALUES (?, ?, ?)", tag, date, value)
			if err != nil {
				log.Println(err.Error())
			}
		}
		start = start.Add(time.Hour)
		end = end.Add(time.Hour)
	}
}
