package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	// Set end time to current 20.01.2023 18:00:00
	end, _ := time.Parse("2006-01-02 15:04:05", "2023-01-22 19:00:00")

	// Connect to MSSQL source database
	sourceDb, err := sql.Open("mssql", "server=139.158.31.1;port=1433;user id=sa;password=!QAZ1qaz12345;database=runtime;TrustServerCertificate=true;encrypt=disable;connection timeout=3000;")
	if err != nil {
		log.Fatal(err)
	}
	defer sourceDb.Close()

	// Connect to MSSQL destination database
	destDb, err := sql.Open("mssql", "server=localhost;port=1433;user id=sa;password=!QAZ1qaz12345;database=runtime;TrustServerCertifier=true;encrypt=disable;connection timeout=3000;")
	if err != nil {
		log.Fatal(err)
	}
	defer destDb.Close()

	// Prepare destination insert statement
	destInsert, err := destDb.Prepare("INSERT INTO runtime.dbo.history (tag_id, [date], [value]) VALUES (?, ?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	defer destInsert.Close()

	// Prepare destination tag select statement
	destSelectTag, err := destDb.Prepare("SELECT id FROM tagname WHERE tag=?")
	if err != nil {
		log.Fatal(err)
	}
	defer destSelectTag.Close()

	// Prepare destination tag insert statement
	destInsertTag, err := destDb.Prepare("INSERT INTO tagname (tag) VALUES (?) SELECT SCOPE_IDENTITY()")
	if err != nil {
		log.Fatal(err)
	}
	defer destInsertTag.Close()

	start := end.Add(-time.Minute)
	for i := 0; i < 1314000; i++ {
		// Copy data from MSSQL source to MSSQL destination for each hour
		startStr := start.Format("2006-01-02 15:04:05")
		endStr := end.Format("2006-01-02 15:04:05")
		fmt.Println(time.Now().Format("2006-01-02 15:04:05"), " : ", startStr, " - ", endStr)

		q := fmt.Sprintf("SELECT h.TagName, h.[DateTime], h.Value FROM history h WHERE h.[DateTime] BETWEEN '%s' AND '%s' and h.tagname like '%%' and h.Value is not null", startStr, endStr)
		rows, err := sourceDb.Query(q)
		if err != nil {
			log.Println(err.Error())
		}
		defer rows.Close()

		// Create slices to store the data
		tagIDs := []int{}
		dates := []time.Time{}
		values := []float32{}

		for rows.Next() {
			var tag string
			var date time.Time
			var value float32
			if err := rows.Scan(&tag, &date, &value); err != nil {
				log.Println(err.Error())
			}

			// Check if tag already exists in tagname table
			var tagID int
			err = destSelectTag.QueryRow(tag).Scan(&tagID)
			if err == sql.ErrNoRows {
				// Tag doesn't exist, insert it into tagname table
				destInsertTag.QueryRow(tag).Scan(&tagID)
				if err != nil {
					log.Println(err.Error())
				}
			} else if err != nil {
				log.Println(err.Error())
			}

			// Append the data to the slices
			tagIDs = append(tagIDs, tagID)
			dates = append(dates, date)
			values = append(values, value)
		}

		// Use bulk insert operation to insert data into destination
		for i := 0; i < len(tagIDs); i++ {
			_, _ = destInsert.Exec(tagIDs[i], dates[i], values[i])
			// if err != nil {
			// 	log.Println(err.Error())
			// }
		}
		start = start.Add(-time.Minute)
		end = end.Add(-time.Minute)
	}
}
