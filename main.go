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
	Start        string // начальная дата и время
	Period       string // длительность периода
	OutputPath   string // директория для сохранения выходных файлов
	Count        int    // количество периодов для обработки
	LastFileName string // имя файла для сохранения/загрузки последнего обработанного периода
	OutputFormat string // формат выходных файлов (json, csv и т.д.)
	Compression  string // метод сжатия (gzip, bzip2 и т.д.)
	NameTemplate string // шаблон имени выходных файлов
	DateFormat   string // формат даты для использования в имени файла
	CsvDelimiter string // разделитель полей в csv-файле
	CsvHeader    bool   // флаг, указывающий, что в csv-файле должен быть заголовок
}

// структура, представляющая приложение
type App struct {
	start                  time.Time     // начальная дата и время
	period                 time.Duration // длительность периода
	outputPath             string        // директория для сохранения выходных файлов
	count                  int           // количество периодов для обработки
	lastFile               string        // имя файла для сохранения/загрузки последнего обработанного периода
	outputFormat           string        // формат выходных файлов (json, csv и т.д.)
	compression            string        // метод сжатия (gzip, bzip2 и т.д.)
	nameTemplate           string        // шаблон имени выходных файлов
	dateFormat             string        // формат даты для использования в имени файла
	csvDelimiter           string        // разделитель полей в csv-файле
	csvHeader              bool          // флаг, указывающий, что в csv-файле должен быть заголовок
	isLast                 bool          // флаг, указывающий, что последний период был загружен из файла
	sourceConnectionString string        // строка подключения к источнику данных
	sourceDb               *sql.DB       // источник данных
	query                  string        // запрос к источнику данных
}

// структура, представляющая одну запись из базы данных
type Record struct {
	TagName  string
	DateTime time.Time
	Value    float64
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

	isLast := false
	if args.Start == "last" {
		s, err := getLastPeriodDate(args.LastFileName)
		if err != nil {
			return nil, err
		}
		args.Start = s
		isLast = true
	}
	start, err := time.Parse("2006-01-02 15:04:05", args.Start)

	if err != nil {
		return nil, fmt.Errorf("ошибка при разборе даты: %v", err)
	}

	period, err := time.ParseDuration(args.Period)
	if err != nil {
		return nil, fmt.Errorf("ошибка при разборе периода: %v", err)
	}
	if period > 24*time.Hour {
		return nil, fmt.Errorf("период не может быть больше 24 часов")
	}

	app := &App{
		start:        start,
		period:       period,
		outputPath:   args.OutputPath,
		count:        args.Count,
		lastFile:     args.LastFileName,
		outputFormat: args.OutputFormat,
		compression:  args.Compression,
		nameTemplate: args.NameTemplate,
		dateFormat:   args.DateFormat,
		csvDelimiter: args.CsvDelimiter,
		csvHeader:    args.CsvHeader,
		isLast:       isLast,
	}

	app.sourceConnectionString = "server=139.158.31.1;port=1433;user id=sa;password=!QAZ1qaz12345;database=runtime;TrustServerCertificate=true;encrypt=disable;connection timeout=3000;"
	app.query = "SELECT h.TagName, h.[DateTime], h.Value FROM history h WHERE h.[DateTime] > '{start}' AND h.[DateTime] <= '{end}' AND h.tagname like '{tag}' AND h.Value is not null;"

	return app, nil
}

// получает параметры командной строки и возвращает структуру CommandLineArgs
func parseCommandLineArgs() (*CommandLineArgs, error) {
	args := &CommandLineArgs{}

	flag.StringVar(&args.Start, "start", "", "начальная дата и время (формат: '2006-01-02 15:04:05' или 'last'), обязательный параметр")
	flag.StringVar(&args.Period, "period", "", "длительность периода (формат: 1h, 5m и т.д.) (не более 24 часов), обязательный параметр")
	flag.StringVar(&args.OutputPath, "output", ".\\", "директория для сохранения выходных файлов, по умолчанию: текущая директория")
	flag.IntVar(&args.Count, "count", 1, "количество периодов для обработки, 0 - обработать все периоды до текущего момента, по умолчанию: 1")
	flag.StringVar(&args.LastFileName, "last", "mssql2file.last", "файл для сохранения/загрузки последнего обработанного периода, по умолчанию: mssql2file.last")
	flag.StringVar(&args.OutputFormat, "format", "json", "формат выходных файлов (json, csv, xml, yaml, toml), по умолчанию: json")
	flag.StringVar(&args.CsvDelimiter, "csv_delimiter", ";", "разделитель полей в csv-файле, по умолчанию: ;")
	flag.BoolVar(&args.CsvHeader, "csv_header", true, "выводить заголовок в csv-файле, по умолчанию: true")
	flag.StringVar(&args.Compression, "compression", "gz", "метод сжатия (none, gz, lz4), по умолчанию: gz")
	flag.StringVar(&args.NameTemplate, "template", "hs_{start}_{end}_{period}.{format}[.]{compressoin}", "шаблон имени выходных файлов, по умолчанию: hs_{start}_{end}_{period}.{format}.{compressoin}")
	flag.StringVar(&args.DateFormat, "date-format", "060102_150405", "формат даты для использования в имени файла, по умолчанию: 060102_150405")

	flag.Parse()

	if args.Start == "" {
		flag.PrintDefaults()
		return nil, fmt.Errorf("необходимо указать начальную дату и время")
	}

	if args.Period == "" || args.Count < 1 {
		flag.PrintDefaults()
		return nil, fmt.Errorf("необходимо указать длительность периода и количество периодов для обработки")
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

	fmt.Println("Общее время: ", time.Since(progStart))

	return nil
}

// подключается к базе данных
func (app *App) connectToDatabase() error {
	var err error
	app.sourceDb, err = sql.Open("mssql", app.sourceConnectionString)
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
func getLastPeriodDate(fileName string) (string, error) {
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

	// lastDate, err := time.Parse("2006-01-02 15:04:05", lastPeriod)
	// if err != nil {
	// 	return "", fmt.Errorf("ошибка парсинга даты последнего обработанного периода: %s", err)
	// }

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
	fmt.Printf("Обработка периода с %s по %s\n", start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"))

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
	fmt.Println("Загрузка данных из базы данных")
	app.query = strings.ReplaceAll(app.query, "{start}", start.Format("2006-01-02 15:04:05"))
	app.query = strings.ReplaceAll(app.query, "{end}", end.Format("2006-01-02 15:04:05"))
	app.query = strings.ReplaceAll(app.query, "{tag}", "%%")

	rows, err := app.sourceDb.Query(app.query)
	if err != nil {
		return nil, fmt.Errorf("ошибка загрузки данных из базы данных: %s", err)
	}
	defer rows.Close()

	data := make([]map[string]interface{}, 0)

	for rows.Next() {
		data = app.writeRow(rows, data)
	}

	if len(data) == 0 {
		fmt.Println("Нет данных для обработки")
		return nil, nil
	}

	return data, nil
}

// сохраняет данные в файл
func (app *App) saveData(start time.Time, end time.Time, data []map[string]interface{}) error {
	fmt.Println("Сохранение данных в файл")
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
	// case "xml":
	// 	encoder := xml.NewEncoder(writer)
	// 	encoder.Encode(data)
	case "yaml":
		encoder := yaml.NewEncoder(writer)
		encoder.Encode(data)
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
	rows := make([][]string, 0)

	if app.csvHeader {
		header := []string{"TagName", "DateTime", "Value"}
		rows = append(rows, header)
	}

	for _, d := range data {
		row := []string{
			d["TagName"].(string),
			d["DateTime"].(time.Time).Format("2006-01-02 15:04:05.000"),
			fmt.Sprintf("%f", d["Value"].(float64)),
		}
		rows = append(rows, row)
	}

	return rows
}
