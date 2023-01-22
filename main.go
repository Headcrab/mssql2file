package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/lib/pq"
)

func main() {
	// Set end time to current 20.01.2023 18:00:00
	end, _ := time.Parse("2006-01-02 15:04:05", "2023-01-24 00:00:00")

	// Connect to MSSQL
	mssqlDb, err := sql.Open("mssql", "server=139.158.31.1;port=1433;user id=sa;password=!QAZ1qaz12345;database=runtime;TrustServerCertificate=true;encrypt=disable;connection timeout=3000;")
	if err != nil {
		log.Fatal(err)
	}
	defer mssqlDb.Close()

	// Connect to PostgreSQL
	pgDb, err := sql.Open("postgres", "host=localhost port=5432 user=postgres password=!QAZ1qaz12345 dbname=runtime sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer pgDb.Close()

	start := end.Add(-time.Hour)
	for i := 0; i < 1314000; i++ {
		// Copy data from MSSQL to PostgreSQL for each hour
		startStr := start.Format("2006-01-02 15:04:05")
		endStr := end.Format("2006-01-02 15:04:05")
		fmt.Println(time.Now().Format("2006-01-02 15:04:05"), " : ", startStr, " - ", endStr)
		q := fmt.Sprintf("SELECT h.TagName, h.[DateTime], h.Value FROM history h WHERE h.[DateTime] BETWEEN '%s' AND '%s' and h.tagname like '%%' and h.Value is not null", startStr, endStr)
		rows, err := mssqlDb.Query(q)
		if err != nil {
			log.Println(err.Error())
		}
		defer rows.Close()

		for rows.Next() {
			var tag string
			var date time.Time
			var value float64
			if err := rows.Scan(&tag, &date, &value); err != nil {
				log.Println(err.Error())
			}

			// Check if tag already exists in tagname table
			var exists bool
			err = pgDb.QueryRow("SELECT EXISTS (SELECT 1 FROM tagname WHERE tag=$1)", tag).Scan(&exists)
			if err != nil {
				log.Println(err.Error())
			}

			var tagID int
			if !exists {
				// Tag doesn't exist, insert it into tagname table
				err = pgDb.QueryRow("INSERT INTO tagname (tag) VALUES ($1) RETURNING id", tag).Scan(&tagID)
				if err != nil {
					log.Println(err.Error())
				}
			} else {
				// Get the ID of the existing tag
				err = pgDb.QueryRow("SELECT id FROM tagname WHERE tag=$1", tag).Scan(&tagID)
				if err != nil {
					log.Println(err.Error())
				}
			}

			// Check if a value with the same tag and date already exists
			var valueExists bool
			err = pgDb.QueryRow("SELECT EXISTS (SELECT 1 FROM history WHERE tag_id=$1 AND date=$2)", tagID, date).Scan(&valueExists)
			if err != nil {
				log.Println(err.Error())
			}

			if !valueExists {
				// Value doesn't exist, insert it into history table
				_, err = pgDb.Exec("INSERT INTO history (tag_id, date, value) VALUES ($1, $2, $3)", tagID, date, value)
				if err != nil {
					log.Println(err.Error())
				}
			}
		}
		start = start.Add(-time.Hour)
		end = end.Add(-time.Hour)
	}
	fmt.Println("Data from MSSQL table 'history' successfully copied to PostgreSQL table 'history'.")
}
