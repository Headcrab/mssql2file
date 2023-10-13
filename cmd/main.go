package main

import (
	"mssql2file/internal/app"

	_ "github.com/denisenkom/go-mssqldb"
)

var Version string
var Name string

func main() {
	app := app.New(Name, Version)
	app.Run()

}
