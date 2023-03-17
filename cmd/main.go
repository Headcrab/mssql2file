package main

import (
	"fmt"
	"mssql2file/internal/configs"
	"mssql2file/internal/exporter"

	"os"

	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	args, err := configs.LoadConfigs()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	exporter, err := exporter.NewExporter(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	err = exporter.Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
