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
	destDb, err := sql.Open("mssql", "server=localhost;port=1433;user id=sa;password=!QAZ1qaz12345;database=runtime;TrustServerCertificate=true;encrypt=disable;connection timeout=3000;")
	if err != nil {
		log.Fatal(err)
	}
	defer destDb.Close()

	start := end.Add(-time.Hour)
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

		for rows.Next() {
			var tag string
			var date time.Time
			var value float32
			if err := rows.Scan(&tag, &date, &value); err != nil {
				log.Println(err.Error())
			}

			// Check if tag already exists in tagname table
			var tagCount int
			err = destDb.QueryRow("SELECT COUNT(*) FROM tagname WHERE tag=?", tag).Scan(&tagCount)
			if err != nil {
				log.Println(err.Error())
			}
			var tagID int
			if tagCount == 0 {
				// Tag doesn't exist, insert it into tagname table
				destDb.QueryRow("INSERT INTO tagname (tag) VALUES (?) SELECT SCOPE_IDENTITY()", tag).Scan(&tagID)
				if err != nil {
					log.Println(err.Error())
				}
			} else {
				// Get the ID of the existing tag
				err = destDb.QueryRow("SELECT id FROM tagname WHERE tag=?", tag).Scan(&tagID)
				if err != nil {
					log.Println(err.Error())
				}
			}

			// Insert data into MSSQL destination database with the tag ID
			// _, err = destDb.Exec("INSERT INTO history (Tag_ID, [Date], Value) VALUES (@tagID, @dt, @value)", sql.Named("tagId", tagID), sql.Named("dt", date), sql.Named("value", value))
			_, _ = destDb.Exec("INSERT INTO runtime.dbo.history (tag_id, [date], [value]) VALUES (?, ?, ?)", tagID, date, value)
			// st, err := destDb.Prepare("INSERT INTO runtime.dbo.history (tag_id, [date], [value]) values (@p1, @p2, @p3)")
			// if err != nil {
			// 	log.Println(err.Error())
			// }
			// _, err = st.Exec(tagID, date, value)
			// if err != nil {
			// 	log.Println(err.Error())
			// }
			// if err != nil {
			// 	log.Println(err.Error())
			// }
		}
		start = start.Add(-time.Hour)
		end = end.Add(-time.Hour)
	}
}
