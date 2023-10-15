package main

import "mssql2file/internal/app"

var Version string
var Name string

func main() {
	app := app.New(Name, Version)
	app.Run()
}
