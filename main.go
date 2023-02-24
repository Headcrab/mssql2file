// package main
package main

import (
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/pierrec/lz4"
	"gopkg.in/yaml.v2"
)

// структура, представляющая параметры командной строки
type CommandLineArgs struct {
	Help             bool   // флаг, указывающий, что нужно вывести справку по параметрам командной строки
	Start            string // начальная дата и время
	Period           string // длительность периода
	OutputPath       string // директория для сохранения выходных файлов
	Count            int    // количество периодов для обработки
	LastFileName     string // имя файла для сохранения/загрузки последнего обработанного периода
	OutputFormat     string // формат выходных файлов (json, csv и т.д.)
	Compression      string // метод сжатия (gzip, bzip2 и т.д.)
	NameTemplate     string // шаблон имени выходных файлов
	DateFormat       string // формат даты для использования в имени файла
	CsvDelimiter     string // разделитель полей в csv-файле
	CsvHeader        bool   // флаг, указывающий, что в csv-файле должен быть заголовок
	ConnectionString string // строка подключения к источнику данных
	Query            string // запрос к источнику данных
	Silient          bool   // флаг, указывающий, что не нужно выводить сообщения в консоль
}

// структура, представляющая приложение
type App struct {
	start            time.Time     // начальная дата и время
	period           time.Duration // длительность периода
	outputPath       string        // директория для сохранения выходных файлов
	count            int           // количество периодов для обработки
	lastFile         string        // имя файла для сохранения/загрузки последнего обработанного периода
	outputFormat     string        // формат выходных файлов (json, csv и т.д.)
	compression      string        // метод сжатия (gzip, bzip2 и т.д.)
	nameTemplate     string        // шаблон имени выходных файлов
	dateFormat       string        // формат даты для использования в имени файла
	csvDelimiter     string        // разделитель полей в csv-файле
	csvHeader        bool          // флаг, указывающий, что в csv-файле должен быть заголовок
	isLast           bool          // флаг, указывающий, что последний период был загружен из файла
	Db               *sql.DB       // источник данных
	connectionString string        // строка подключения к источнику данных
	query            string        // запрос к источнику данных
	silient          bool          // флаг, указывающий, что не нужно выводить сообщения в консоль
}

func main() {
	args, err := parseCommandLineArgs()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	app, err := NewApp(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	err = app.Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

// создает новое приложение с заданными параметрами командной строки
func NewApp(args *CommandLineArgs) (*App, error) {

	app := &App{
		outputPath:       args.OutputPath,
		count:            args.Count,
		lastFile:         args.LastFileName,
		outputFormat:     args.OutputFormat,
		compression:      args.Compression,
		nameTemplate:     args.NameTemplate,
		dateFormat:       args.DateFormat,
		csvDelimiter:     args.CsvDelimiter,
		csvHeader:        args.CsvHeader,
		connectionString: args.ConnectionString,
		query:            args.Query,
		silient:          args.Silient,
	}

	var err error
	app.isLast = false
	if args.Start == "last" {
		args.Start, err = app.getLastPeriodDate(args.LastFileName)
		if err != nil {
			return nil, err
		}
		app.isLast = true
	}

	app.start, err = time.Parse("2006-01-02 15:04:05", args.Start)
	if err != nil {
		return nil, fmt.Errorf("ошибка при разборе даты: %v", err)
	}

	app.period, err = time.ParseDuration(args.Period)
	if err != nil {
		return nil, fmt.Errorf("ошибка при разборе периода: %v", err)
	}
	if app.period > 24*time.Hour {
		return nil, fmt.Errorf("период не может быть больше 24 часов")
	}

	return app, nil
}

// получает параметры командной строки и возвращает структуру CommandLineArgs
func parseCommandLineArgs() (*CommandLineArgs, error) {
	args := &CommandLineArgs{}

	flag.BoolVar(&args.Silient, "silient", true, "флаг, указывающий, что не нужно выводить сообщения в консоль")
	flag.StringVar(&args.Start, "start", "last", "начальная дата и время (формат: '2006-01-02 15:04:05' или 'last'), по умолчанию: last")
	flag.StringVar(&args.Period, "period", "1m", "длительность периода (формат: 1h, 5m и т.д.) (не более 24 часов), по умолчанию: 1m")
	flag.StringVar(&args.OutputPath, "output", ".\\", "директория для сохранения выходных файлов, по умолчанию: текущая директория")
	flag.IntVar(&args.Count, "count", 1, "количество периодов для обработки, 0 - обработать все периоды до текущего момента, по умолчанию: 1")
	flag.StringVar(&args.LastFileName, "last", "mssql2file.last", "файл для сохранения/загрузки последнего обработанного периода, по умолчанию: mssql2file.last")
	flag.StringVar(&args.OutputFormat, "format", "json", "формат выходных файлов (json, csv, xml, yaml, toml), по умолчанию: json")
	flag.StringVar(&args.CsvDelimiter, "csv_delimiter", ";", "разделитель полей в csv-файле, по умолчанию: ;")
	flag.BoolVar(&args.CsvHeader, "csv_header", true, "выводить заголовок в csv-файле, по умолчанию: true")
	flag.StringVar(&args.Compression, "compression", "gz", "метод сжатия (none, gz, lz4), по умолчанию: gz")
	flag.StringVar(&args.NameTemplate, "template", "hs_{start}_{end}_{period}.{format}[.]{compressoin}", "шаблон имени выходных файлов, по умолчанию: hs_{start}_{end}_{period}.{format}.{compressoin}")
	flag.StringVar(&args.DateFormat, "date-format", "060102_150405", "формат даты для использования в имени файла, по умолчанию: 060102_150405")
	flag.StringVar(&args.ConnectionString, "connection-string", "server=139.158.31.1;port=1433;user id=sa;password=!QAZ1qaz12345;database=runtime;TrustServerCertificate=true;encrypt=disable;connection timeout=3000;", "строка подключения к БД MSSQL, по умолчанию HS0")
	flag.StringVar(&args.Query, "query", "SELECT TagName, format(DateTime, 'yyyy-MM-dd HH:mm:ss.fff') as DateTime, Value FROM history WHERE DateTime > '{start}' AND DateTime <= '{end}' AND tagname like '{tag}' AND Value is not null;", "запрос к БД MSSQL, по умолчанию: SELECT TagName, format(DateTime, 'yyyy-MM-dd HH:mm:ss.fff') as DateTime, Value FROM history WHERE DateTime > '{start}' AND DateTime <= '{end}' AND tagname like '{tag}' AND Value is not null;")

	flag.Parse()

	if args.Help {
		flag.PrintDefaults()
		return nil, fmt.Errorf("помощь по параметрам командной строки")
	}

	return args, nil
}

// генерирует имя файла для выходного файла
func (app *App) generateFileName(start time.Time, end time.Time) string {
	fileName := app.nameTemplate
	fileName = strings.ReplaceAll(fileName, "{period}", app.period.String())
	fileName = strings.ReplaceAll(fileName, "{start}", start.Format(app.dateFormat))
	fileName = strings.ReplaceAll(fileName, "{end}", end.Format(app.dateFormat))
	fileName = strings.ReplaceAll(fileName, "{format}", app.outputFormat)
	if app.compression == "none" {
		fileName = strings.ReplaceAll(fileName, "{compressoin}", "")
		fileName = strings.ReplaceAll(fileName, "[.]", "")
	} else {
		fileName = strings.ReplaceAll(fileName, "{compressoin}", app.compression)
		fileName = strings.ReplaceAll(fileName, "[.]", ".")
	}
	return app.outputPath + "/" + fileName
}

// запускает приложение
func (app *App) Run() error {
	err := app.connectToDatabase()
	if err != nil {
		return err
	}

	progStart := time.Now()

	err = app.processAllPeriods(app.start)
	if err != nil {
		return err
	}

	if !app.silient {
		// время выполнения программы в формате 1h2m3s
		fmt.Println("Время обработки: ", time.Since(progStart).Truncate(time.Second))
	}

	return nil
}

// подключается к базе данных
func (app *App) connectToDatabase() error {
	var err error
	app.Db, err = sql.Open("mssql", app.connectionString)
	if err != nil {
		return fmt.Errorf("ошибка подключения к базе данных: %s", err)
	}
	return nil
}

// сохраняет дату последнего обработанного периода в файл
func (app *App) saveLastPeriodDate(end time.Time) error {
	file, err := os.Create(app.lastFile)
	if err != nil {
		return fmt.Errorf("ошибка создания файла последнего обработанного периода: %s", err)
	}
	defer file.Close()

	_, err = file.WriteString(end.Format("2006-01-02 15:04:05"))
	if err != nil {
		return fmt.Errorf("ошибка записи в файл последнего обработанного периода: %s", err)
	}

	return nil
}

// получает дату последнего обработанного периода из файла
func (app *App) getLastPeriodDate(fileName string) (string, error) {
	// проверряем существование файла
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) && app.isLast {
		app.saveLastPeriodDate(time.Now().Add(-app.period))
		return "", fmt.Errorf("файл последнего обработанного периода не найден, создан новый")
	}

	file, err := os.Open(fileName)
	if err != nil {
		return "", fmt.Errorf("ошибка открытия файла последнего обработанного периода: %s", err)
	}
	defer file.Close()

	var lastPeriodD string
	var lastPeriodT string
	_, err = fmt.Fscanf(file, "%s %s", &lastPeriodD, &lastPeriodT)
	if err != nil {
		return "", fmt.Errorf("ошибка чтения файла последнего обработанного периода: %s", err)
	}

	return lastPeriodD + " " + lastPeriodT, nil
}

// обрабатывает все периоды
func (app *App) processAllPeriods(start time.Time) error {
	if app.count == 0 {
		app.count = 1000000
	}
	for i := 0; i < app.count; i++ {
		end := start.Add(app.period)
		// если конец периода после текущего момента, то прекращаем обработку
		if end.After(time.Now()) {
			break
		}

		err := app.processPeriod(start, end)
		if err != nil {
			return err
		}

		start = end
	}

	return nil
}

// обрабатывает один период
func (app *App) processPeriod(start time.Time, end time.Time) error {
	if !app.silient {
		fmt.Printf("Обработка периода с %s по %s\n", start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"))
	}

	data, err := app.loadData(start, end)
	if err != nil {
		return err
	}

	err = app.saveData(start, end, data)
	if err != nil {
		return err
	}

	return nil
}

// загружает данные из базы данных
func (app *App) loadData(start time.Time, end time.Time) ([]map[string]interface{}, error) {
	beg := time.Now()
	if !app.silient {
		fmt.Print("Загрузка данных из базы данных")
	}
	app.query = strings.ReplaceAll(app.query, "{start}", start.Format("2006-01-02 15:04:05"))
	app.query = strings.ReplaceAll(app.query, "{end}", end.Format("2006-01-02 15:04:05"))
	app.query = strings.ReplaceAll(app.query, "{tag}", "%%")

	rows, err := app.Db.Query(app.query)
	if err != nil {
		return nil, fmt.Errorf("ошибка загрузки данных из базы данных: %s", err)
	}
	defer rows.Close()

	data := make([]map[string]interface{}, 0)

	for rows.Next() {
		data = app.writeRow(rows, data)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("нет данных для обработки")
	}

	if !app.silient {
		fmt.Printf(" - %d строк за %s\n", len(data), time.Since(beg).Truncate(time.Second))
	}
	return data, nil
}

// сохраняет данные в файл
func (app *App) saveData(start time.Time, end time.Time, data []map[string]interface{}) error {
	beg := time.Now()
	if !app.silient {
		fmt.Print("Сохранение данных в файл")
	}
	fileName := app.generateFileName(start, end)

	// проверяем путь к выходному файлу и создаем его, если не существует
	if _, err := os.Stat(app.outputPath); os.IsNotExist(err) {
		err := os.MkdirAll(app.outputPath, 0755)
		if err != nil {
			return fmt.Errorf("неверный путь к выходному файлу: %s", err)
		}
	}

	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("ошибка создания файла: %s", err)
	}
	defer file.Close()

	var writer io.Writer
	switch app.compression {
	case "gz":
		writer = gzip.NewWriter(file)
		defer writer.(*gzip.Writer).Close()
	case "lz4":
		writer = lz4.NewWriter(file)
		defer writer.(*lz4.Writer).Close()
	default:
		writer = file
	}

	switch app.outputFormat {
	case "csv":
		encoder := csv.NewWriter(writer)
		// первый символ из app.csvDelimiter
		encoder.Comma = rune(app.csvDelimiter[0])
		encoder.WriteAll(app.convertDataToCsv(data))
	case "json":
		encoder := json.NewEncoder(writer)
		encoder.Encode(data)
	// todo: не реализовано
	// case "xml":
	// 	encoder := xml.NewEncoder(writer)
	// 	encoder.Encode(data)
	case "yaml":
		encoder := yaml.NewEncoder(writer)
		encoder.Encode(data)
	// todo: не реализовано
	// case "toml":
	// 	encoder := toml.NewEncoder(writer)
	// 	encoder.Encode(data)
	default:
		return fmt.Errorf("неизвестный формат выходных данных: %s", app.outputFormat)
	}

	err = app.saveLastPeriodDate(end)
	if err != nil {
		return err
	}

	if !app.silient {
		fmt.Printf(" - %s\n", time.Since(beg).Truncate(time.Second))
	}

	return nil
}

// записывает строку в массив данных
func (app *App) writeRow(rows *sql.Rows, data []map[string]interface{}) []map[string]interface{} {
	columns, err := rows.Columns()
	if err != nil {
		panic(fmt.Errorf("ошибка получения столбцов: %s", err))
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	err = rows.Scan(valuePtrs...)
	if err != nil {
		panic(fmt.Errorf("ошибка чтения строк: %s", err))
	}

	row := make(map[string]interface{})
	for i, col := range columns {
		var v interface{}
		val := values[i]
		b, ok := val.([]byte)
		if ok {
			v = string(b)
		} else {
			v = val
		}
		row[col] = v
	}
	return append(data, row)
}

// конвертирует данные из базы данных в формат CSV
func (app *App) convertDataToCsv(data []map[string]interface{}) [][]string {
	rows := make([][]string, len(data))

	if app.csvHeader {
		header := make([]string, 0)
		for k := range data[0] {
			header = append(header, k)
		}
		rows[0] = header
		rows = append(rows, []string{})
	}

	for i, d := range data {
		row := []string{}
		for _, v := range d {
			row = append(row, fmt.Sprintf("%v", v))
		}
		if !app.csvHeader {
			rows[i] = row
		} else {
			rows[i+1] = row
		}
	}

	return rows
}
