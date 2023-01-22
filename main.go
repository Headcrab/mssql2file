package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// Set end time to current 20.01.2023 18:00:00
	end, _ := time.Parse("2006-01-02 15:04:05", "2022-12-16 00:02:00")

	// Connect to MSSQL
	mssqlDb, err := sql.Open("mssql", "server=139.158.31.1;port=1433;user id=sa;password=!QAZ1qaz12345;database=runtime;TrustServerCertificate=true;encrypt=disable;connection timeout=3000;")
	if err != nil {
		log.Fatal(err)
	}
	defer mssqlDb.Close()

	// Connect to MongoDB
	mongoClient, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}
	err = mongoClient.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	defer mongoClient.Disconnect(context.TODO())

	historyColl := mongoClient.Database("runtime").Collection("history")

	start := end.Add(-time.Hour * 24)
	for i := 0; i < 365; i++ {
		// Copy data from MSSQL to MongoDB for each hour
		startStr := start.Format("2006-01-02 15:04:05")
		endStr := end.Format("2006-01-02 15:04:05")
		fmt.Println(time.Now().Format("2006-01-02 15:04:05"), " : ", startStr, " - ", endStr)
		q := fmt.Sprintf("SELECT h.TagName, h.[DateTime], h.Value FROM history h WHERE h.[DateTime] BETWEEN '%s' AND '%s' and h.tagname like '%%' and h.Value is not null", startStr, endStr)
		rows, err := mssqlDb.Query(q)
		if err != nil {
			log.Println(err.Error())
		}
		defer rows.Close()

		var docs []interface{}
		for rows.Next() {
			var tag string
			var date time.Time
			var value float64
			if err := rows.Scan(&tag, &date, &value); err != nil {
				log.Println(err.Error())
			}

			docs = append(docs, bson.M{"tag": tag, "date": date, "value": value})
		}

		// Insert multiple documents at once
		_, err = historyColl.InsertMany(context.TODO(), docs)
		if err != nil {
			log.Println(err.Error())
		}
		start, end = start.Add(-time.Hour*24), end.Add(-time.Hour*24)
	}
	fmt.Println("Data from MSSQL table 'history' successfully copied to MongoDB collection 'history'.")
}
