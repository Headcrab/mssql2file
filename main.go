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
	end, _ := time.Parse("2006-01-02 15:04:05", "2023-01-23 12:00:00")

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

	// Prepare destination tag insert statement
	destInsertTag, err := destDb.Prepare("INSERT INTO tagname (tag) VALUES (?) SELECT SCOPE_IDENTITY()")
	if err != nil {
		log.Fatal(err)
	}
	defer destInsertTag.Close()

	// Create a map to cache the tag names and their corresponding IDs
	tagCache := make(map[string]int)

	period := -time.Minute * 5
	start := end.Add(period)
	for i := 0; i < 8760; i++ {
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

		for rows.Next() {
			var tag string
			var date time.Time
			var value float32
			if err := rows.Scan(&tag, &date, &value); err != nil {
				log.Println(err.Error())
			}

			// Check if tag is already in cache
			var tagID int
			var ok bool
			if tagID, ok = tagCache[tag]; !ok {
				// Tag not in cache, check the destination database
				destInsertTag.QueryRow(tag).Scan(&tagID)
				// if err != nil {
				// 	log.Println(err.Error())
				// }
				// Add the new tag to the cache
				tagCache[tag] = tagID
			}

			// Insert data into destination with the tag ID
			destInsert.Exec(tagID, date, value)
			// if err != nil {
			// 	log.Println(err.Error())
			// }
		}
		start = start.Add(period)
		end = end.Add(period)
	}
}
