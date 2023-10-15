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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aa201e5 (go-mssqldb moved)
	// загрузка конфигурации
	conf := config.New()
	conf.SetPrintFunc(func() {
		fmt.Fprintln(os.Stdout, app.name, " v:", app.version)
	})
	err := conf.Load()
<<<<<<< HEAD
	if err != nil {
=======

	fmt.Fprintln(os.Stdout, app.name, " v:", app.version)

	config, err := config.Load()
	if err != nil {
		// log.Fatalf("failed to load config: %s", err)
>>>>>>> 448a933 (app.ver added)
=======
	if err != nil {
>>>>>>> aa201e5 (go-mssqldb moved)
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

<<<<<<< HEAD
<<<<<<< HEAD
	// создание экспортера
	exporter, err := exporter.Create(conf)
=======
	exporter, err := exporter.Create(config)
>>>>>>> 448a933 (app.ver added)
=======
	// создание экспортера
	exporter, err := exporter.Create(conf)
>>>>>>> aa201e5 (go-mssqldb moved)
	if err != nil {
		// log.Fatalf("failed to create exporter: %s", err)
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

<<<<<<< HEAD
<<<<<<< HEAD
	// запуск экспортера
=======
>>>>>>> 448a933 (app.ver added)
=======
	// запуск экспортера
>>>>>>> aa201e5 (go-mssqldb moved)
	err = exporter.Run()
	if err != nil {
		// log.Fatalf("failed to run exporter: %s", err)
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	return nil
}
