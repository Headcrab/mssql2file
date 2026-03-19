package main

import (
	"fmt"
	"mssql2file/internal/app"
	"mssql2file/internal/apperrors"
	"os"
	"strings"
	"time"
)

var Version string
var Name string

func main() {
	app := app.New(Name, Version)
	if err := app.Run(); err != nil {
		helpPrefix := strings.Replace(apperrors.CommandLineHelp, "%s", "", 1)
		if strings.HasPrefix(err.Error(), helpPrefix) {
			return
		}
		fmt.Fprintf(os.Stderr, "[%s] Ошибка | %s\n", time.Now().Format("02.01.2006 15:04:05"), err)
		os.Exit(1)
	}
}
