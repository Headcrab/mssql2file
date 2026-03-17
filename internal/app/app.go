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
	// загрузка конфигурации
	conf := config.New()
	conf.SetPrintFunc(func() {
		fmt.Fprintln(os.Stdout, app.name, " v:", app.version)
	})
	err := conf.Load()
	if err != nil {
		return err
	}

	// создание экспортера
	exporter, err := exporter.Create(conf)
	if err != nil {
		return err
	}

	// запуск экспортера
	err = exporter.Run()
	if err != nil {
		return err
	}
	return nil
}
