package main

import (
	"log/slog"
	"mssql2file/internal/app"
	"mssql2file/internal/apperrors"
	"os"
	"strings"
)

var Version string
var Name string

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	app := app.New(Name, Version)
	if err := app.Run(); err != nil {
		helpPrefix := strings.Replace(apperrors.CommandLineHelp, "%s", "", 1)
		if strings.HasPrefix(err.Error(), helpPrefix) {
			return
		}
		slog.Error("Application run failed", "error", err)
		os.Exit(1)
	}
}
