package main

import (
	"log/slog"
	"mssql2file/internal/app"
	"os"
)

var Version string
var Name string

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	app := app.New(Name, Version)
	if err := app.Run(); err != nil {
		slog.Error("Application run failed", "error", err)
		os.Exit(1)
	}
}
