package app

import (
	"fmt"
	"mssql2file/internal/config"
	"mssql2file/internal/exporter"
	"os"
)

type App struct {
	name    string
	version string
}

func New(Name string, Version string) *App {
	return &App{
		name:    Name,
		version: Version,
	}
}

func (app *App) Run() error {

	fmt.Fprintln(os.Stdout, app.name, " v:", app.version)

	config, err := config.Load()
	if err != nil {
		// log.Fatalf("failed to load config: %s", err)
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	exporter, err := exporter.Create(config)
	if err != nil {
		// log.Fatalf("failed to create exporter: %s", err)
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	err = exporter.Run()
	if err != nil {
		// log.Fatalf("failed to run exporter: %s", err)
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	return nil
}
