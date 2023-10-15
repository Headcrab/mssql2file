package apperrors

import (
	"fmt"
)

func New(err string, param string) (e error) {
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aa201e5 (go-mssqldb moved)
	if param != "" {
		return fmt.Errorf(err, param)
	}
	return fmt.Errorf(err)
<<<<<<< HEAD
=======
	return fmt.Errorf(err, param)
>>>>>>> 448a933 (app.ver added)
=======
>>>>>>> aa201e5 (go-mssqldb moved)
}

const (
	BeginDateParse             = "ошибка при разборе даты: %s"
	BeginDateNotSet            = "не задана дата начала обработки"
<<<<<<< HEAD
<<<<<<< HEAD
	CommandLineHelp            = "помощь по параметрам командной строки %s"
=======
	CommandLineHelp            = "помощь по параметрам командной строки"
>>>>>>> 448a933 (app.ver added)
=======
	CommandLineHelp            = "помощь по параметрам командной строки %s"
>>>>>>> aa201e5 (go-mssqldb moved)
	DbConnection               = "ошибка подключения к базе данных: %s"
	DbQuery                    = "ошибка выполнения запроса к базе данных: %s"
	DbColumns                  = "ошибка получения списка колонок: %s"
	DbScan                     = "ошибка при чтении данных из базы данных: %s"
	DbNoData                   = "нет данных для обработки"
	LastPeriodWrite            = "ошибка при записи последнего периода: %s"
	LastPeriodRead             = "ошибка при чтении последнего периода: %s"
	LastPeriodParse            = "ошибка при разборе последнего периода: %s"
	LastPeriodFileOpen         = "ошибка при открытии файла последнего периода: %s"
	LastPeriodFolderCreate     = "ошибка при создании папки для файла последнего периода: %s"
	LastPeriodFileCreate       = "ошибка при создании файла последнего периода: %s"
	OutputWrongPath            = "неверный путь к файлу: %s"
	OutputCreateFile           = "ошибка при создании файла: %s"
	PeriodParse                = "ошибка при разборе периода: %s"
	PeriodTooLong              = "период не может быть больше 24 часов"
	UnsupportedFormat          = "формат не '%s' поддерживается"
	UnsupportedLocationType    = "тип '%s' не поддерживается"
	UnsupportedCompressionType = "формат сжатия '%s' не поддерживается"
	Timeout                    = "ошибка таймаута: %s"
)
