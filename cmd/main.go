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
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/pierrec/lz4"
	"gopkg.in/yaml.v2"
)

// структура, представляющая параметры командной строки
type CommandLineArgs struct {
	// LastFileName     string // имя файла для сохранения/загрузки последнего обработанного периода
	Help              bool   // флаг, указывающий, что нужно вывести справку по параметрам командной строки
	Start             string // начальная дата и время
	Period            string // длительность периода
	Output            string // директория для сохранения выходных файлов
	Count             int    // количество периодов для обработки
	Output_format     string // формат выходных файлов (json, csv и т.д.)
	Compression       string // метод сжатия (gzip, bzip2 и т.д.)
	Template          string // шаблон имени выходных файлов
	Date_format       string // формат даты для использования в имени файла
	Csv_delimiter     string // разделитель полей в csv-файле
	Csv_header        bool   // флаг, указывающий, что в csv-файле должен быть заголовок
	Connection_string string // строка подключения к источнику данных
	Query             string // запрос к источнику данных
	Silient           bool   // флаг, указывающий, что не нужно выводить сообщения в консоль
	Config_file       string // файл конфигурации
	Last_period_end   string // дата и время окончания последнего периода
}

func (args *CommandLineArgs) add(source *CommandLineArgs) {
	v := reflect.ValueOf(args).Elem()
	s := reflect.ValueOf(source).Elem()
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).IsZero() {
			v.Field(i).Set(s.Field(i))
		}
	}
}

// структура, представляющая приложение
type Exporter struct {
	// lastFile         string        // имя файла для сохранения/загрузки последнего обработанного периода
	start            time.Time     // начальная дата и время
	period           time.Duration // длительность периода
	outputPath       string        // директория для сохранения выходных файлов
	count            int           // количество периодов для обработки
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
	columns          []string      // список колонок
	configFile       string        // файл конфигурации
	lastPeriodEnd    string        // дата и время окончания последнего периода
}

func main() {
	args, err := parseCommandLineArgs()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	exporter, err := NewExporter(args)
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

// создает новое приложение с заданными параметрами командной строки
func NewExporter(args *CommandLineArgs) (*Exporter, error) {

	app := &Exporter{
		// lastFile:         args.LastFileName,
		outputPath:       args.Output,
		count:            args.Count,
		outputFormat:     args.Output_format,
		compression:      args.Compression,
		nameTemplate:     args.Template,
		dateFormat:       args.Date_format,
		csvDelimiter:     args.Csv_delimiter,
		csvHeader:        args.Csv_header,
		connectionString: args.Connection_string,
		query:            args.Query,
		silient:          args.Silient,
		configFile:       args.Config_file,
		lastPeriodEnd:    args.Last_period_end,
	}

	if args.Last_period_end == "" && args.Start == "last" {
		return nil, fmt.Errorf("не задана дата начала обработки")
	}
	var err error
	app.isLast = false
	if args.Start == "last" {
		app.start, err = time.Parse("2006-01-02 15:04:05", args.Last_period_end)
		if err != nil {
			return nil, fmt.Errorf("ошибка при разборе даты: %v", err)
		}
		app.isLast = true
	} else {

		app.start, err = time.Parse("2006-01-02 15:04:05", args.Start)
		if err != nil {
			return nil, fmt.Errorf("ошибка при разборе даты: %v", err)
		}
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

	flag.BoolVar(&args.Silient, "silient", false, "флаг, указывающий, что не нужно выводить сообщения в консоль")
	flag.StringVar(&args.Start, "start", "", "начальная дата и время (формат: '2006-01-02 15:04:05' или 'last'), по умолчанию: last")
	flag.StringVar(&args.Period, "period", "", "длительность периода (формат: 1h, 5m и т.д.) (не более 24 часов), по умолчанию: 1m")
	flag.StringVar(&args.Output, "output", "", "директория для сохранения выходных файлов, по умолчанию: текущая директория")
	flag.StringVar(&args.Template, "name", "", "шаблон имени выходных файлов, по умолчанию: hs_{start}_{end}_{period}.{format}.{compression}")
	flag.IntVar(&args.Count, "count", 0, "количество периодов для обработки, 0 - обработать все периоды до текущего момента, по умолчанию: 1")
	flag.StringVar(&args.Output_format, "format", "", "формат выходных файлов (json, csv, xml, yaml, toml), по умолчанию: json")
	flag.StringVar(&args.Csv_delimiter, "csv_delimiter", "", "разделитель полей в csv-файле, по умолчанию: ;")
	flag.BoolVar(&args.Csv_header, "csv_header", false, "выводить заголовок в csv-файле, по умолчанию: false")
	flag.StringVar(&args.Compression, "compression", "", "метод сжатия (none, gz, lz4), по умолчанию: gz")
	flag.StringVar(&args.Date_format, "date_format", "", "формат даты для использования в имени файла, по умолчанию: 060102_150405")
	flag.StringVar(&args.Connection_string, "connection_string", "", "строка подключения к БД MSSQL, по умолчанию HS0")
	flag.StringVar(&args.Query, "query", "", "запрос к БД MSSQL")
	flag.StringVar(&args.Config_file, "config", "", "файл конфигурации, по умолчанию: mssql2file.cfg")
	flag.StringVar(&args.Last_period_end, "last_period_end", "", "дата и время окончания последнего периода, по умолчанию: не используется")

	flag.Parse()

	if args.Help {
		flag.PrintDefaults()
		return nil, fmt.Errorf("помощь по параметрам командной строки")
	}

	err := mergeArgs(args)
	if err != nil {
		return nil, err
	}

	return args, nil
}

func readConfigFile(filePath string) (CommandLineArgs, error) {
	configFile, err := os.Open(filePath)
	if err != nil {
		return CommandLineArgs{}, err
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	args := &CommandLineArgs{}
	if err = jsonParser.Decode(args); err != nil {
		return CommandLineArgs{}, err
	}

	return *args, nil
}

func mergeArgs(args *CommandLineArgs) error {
	// Чтение переменных окружения
	envArgs := CommandLineArgs{
		Silient:           getEnvBool("M2F_SILIENT", false),
		Start:             getEnvString("M2F_START", "last"),
		Period:            getEnvString("M2F_PERIOD", "1m"),
		Output:            getEnvString("M2F_OUTPUT", ""),
		Count:             getEnvInt("M2F_COUNT", 0),
		Output_format:     getEnvString("M2F_FORMAT", "json"),
		Csv_delimiter:     getEnvString("M2F_CSV_DELIMITER", ","),
		Csv_header:        getEnvBool("M2F_CSV_HEADER", false),
		Compression:       getEnvString("M2F_COMPRESSION", "gz"),
		Template:          getEnvString("M2F_TEMPLATE", "hs_{start}_{end}_{period}.{format}.{compression}"),
		Date_format:       getEnvString("M2F_DATE_FORMAT", "060102_150405"),
		Connection_string: getEnvString("M2F_CONNECTION_STRING", "server=139.158.31.1;port=1433;user id=sa;password=!QAZ1qaz12345;database=runtime;TrustServerCertificate=true;encrypt=disable;connection timeout=3000;"),
		Query:             getEnvString("M2F_QUERY", "SELECT TagName, format(DateTime, 'yyyy-MM-dd HH:mm:ss.fff') as DateTime, Value FROM history WHERE DateTime > '{start}' AND DateTime <= '{end}' AND TagName like '{tag}' AND Value is not null;"),
		Config_file:       getEnvString("M2F_CONFIG", "mssql2file.cfg"),
		Last_period_end:   getEnvString("M2F_LAST_PERIOD_END", ""),
	}

	if args.Config_file == "" {
		args.Config_file = envArgs.Config_file
	}
	configFileArgs, _ := readConfigFile(args.Config_file)

	// Объединение значений

	args.add(&configFileArgs)
	args.add(&envArgs)

	return nil
}

// getEnvString возвращает значение переменной окружения как строку.
// Если переменная окружения не задана, возвращает значение по умолчанию.
func getEnvString(key, defValue string) string {
	value, exists := os.LookupEnv(key)
	if exists {
		return value
	}
	return defValue
}

// getEnvInt возвращает значение переменной окружения как целое число.
// Если переменная окружения не задана, возвращает значение по умолчанию.
func getEnvInt(key string, defValue int) int {
	value, exists := os.LookupEnv(key)
	if exists {
		intValue, err := strconv.Atoi(value)
		if err == nil {
			return intValue
		}
	}
	return defValue
}

// getEnvBool возвращает значение переменной окружения как логическое значение.
// Если переменная окружения не задана, возвращает значение по умолчанию.
func getEnvBool(key string, defValue bool) bool {
	value, exists := os.LookupEnv(key)
	if exists {
		boolValue, err := strconv.ParseBool(value)
		if err == nil {
			return boolValue
		}
	}
	return defValue
}

// генерирует имя файла для выходного файла
func (exporter *Exporter) generateFileName(start time.Time, end time.Time) string {
	// check / in the end of output path
	if exporter.outputPath != "" && exporter.outputPath[len(exporter.outputPath)-1:] != "/" {
		exporter.outputPath += "/"
	}
	fileName := exporter.outputPath + exporter.nameTemplate
	fileName = strings.ReplaceAll(fileName, "{period}", exporter.period.String())
	fileName = strings.ReplaceAll(fileName, "{start}", start.Format(exporter.dateFormat))
	fileName = strings.ReplaceAll(fileName, "{end}", end.Format(exporter.dateFormat))
	fileName = strings.ReplaceAll(fileName, "{format}", exporter.outputFormat)
	if exporter.compression == "none" {
		fileName = strings.ReplaceAll(fileName, ".{compression}", "")
		// fileName = strings.ReplaceAll(fileName, "[.]", "")
	} else {
		fileName = strings.ReplaceAll(fileName, "{compression}", exporter.compression)
		// fileName = strings.ReplaceAll(fileName, "[.]", ".")
	}
	return fileName
}

// запускает приложение
func (exporter *Exporter) Run() error {
	err := exporter.connectToDatabase()
	if err != nil {
		return err
	}

	progStart := time.Now()

	err = exporter.processAllPeriods(exporter.start)
	if err != nil {
		return err
	}

	if !exporter.silient {
		// время выполнения программы в формате 1h2m3s
		fmt.Println("Время обработки: ", time.Since(progStart).Truncate(time.Second))
	}

	return nil
}

// подключается к базе данных
func (exporter *Exporter) connectToDatabase() error {
	var err error
	exporter.Db, err = sql.Open("mssql", exporter.connectionString)
	if err != nil {
		return fmt.Errorf("ошибка подключения к базе данных: %s", err)
	}
	return nil
}

// сохраняет дату последнего обработанного периода в файл
func (exporter *Exporter) saveLastPeriodDate(end time.Time) error {
	// если файл существует, то пишем в него дату последнего обработанного периода
	// если файла не существует, то создаем его и пишем в него дату последнего обработанного периода app.lastPeriodDate

	// получаем путь к выходному файлу
	outputPath := filepath.Dir(exporter.configFile)
	var file *os.File
	var config map[string]interface{}
	// проверяем существование файла и создаем его, если не существует
	if _, err := os.Stat(exporter.configFile); os.IsNotExist(err) {
		// проверяем существование папки
		if _, err := os.Stat(outputPath); os.IsNotExist(err) {
			// создаем папку
			err = os.MkdirAll(outputPath, 0755)
			if err != nil {
				return fmt.Errorf("ошибка создания папки для файла последнего обработанного периода: %s", err)
			}
		}
		// создаем файл
		file, err = os.Create(exporter.configFile)
		if err != nil {
			return fmt.Errorf("ошибка создания файла последнего обработанного периода: %s", err)
		}
		defer file.Close()
	} else {
		// читаем существующие данные из файла
		file, err = os.Open(exporter.configFile)
		if err != nil {
			return fmt.Errorf("ошибка открытия файла последнего обработанного периода: %s", err)
		}
		defer file.Close()
		// читаем все существующие данные из файла в формате json
		err = json.NewDecoder(file).Decode(&config)
		if err != nil {
			return fmt.Errorf("ошибка преобразования данных файла последнего обработанного периода: %s", err)
		}
		// close file
		file.Close()
		// open file for write
		file, err = os.OpenFile(exporter.configFile, os.O_RDWR, 0755)
		if err != nil {
			return fmt.Errorf("ошибка открытия файла последнего обработанного периода на запись: %s", err)
		}
		defer file.Close()
	}
	// добавляем в структуру новую дату последнего обработанного периода
	if config == nil {
		config = make(map[string]interface{})
	}
	// пишем в файл дату последнего обработанного периода в json 'Last_period_end'
	config["Last_period_end"] = end.Format("2006-01-02 15:04:05")
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	err := encoder.Encode(config)
	if err != nil {
		return fmt.Errorf("ошибка записи в файл последнего обработанного периода: %s", err)
	}

	return nil
}

// обрабатывает все периоды
func (exporter *Exporter) processAllPeriods(start time.Time) error {
	now, _ := time.Parse("2006-01-02 15:04:05", time.Now().Format("2006-01-02 15:04:05"))
	if exporter.count == 0 {
		// рассчитываем количество периодов с учетом периода и даты начала и текущего момента
		exporter.count = int(now.Sub(start).Minutes() / exporter.period.Minutes())
	}
	for i := 0; i < exporter.count; i++ {
		end := start.Add(exporter.period)
		// если конец периода после текущего момента то выходим из цикла
		if end.After(now) {
			break
		}

		err := exporter.processPeriod(start, end)
		if err != nil {
			return err
		}

		start = end
	}

	return nil
}

// обрабатывает один период
func (exporter *Exporter) processPeriod(start time.Time, end time.Time) error {
	// если start > end, то меняем их местами
	if start.After(end) {
		start, end = end, start
	}

	if !exporter.silient {
		fmt.Printf("Обработка периода с %s по %s\n", start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"))
	}

	data, err := exporter.loadData(start, end)
	if err != nil {
		return err
	}

	err = exporter.saveData(start, end, data)
	if err != nil {
		return err
	}

	return nil
}

// загружает данные из базы данных
func (exporter *Exporter) loadData(start time.Time, end time.Time) ([]map[string]interface{}, error) {
	beg := time.Now()
	if !exporter.silient {
		fmt.Print("Загрузка данных из базы данных ")
	}
	exporter.query = strings.ReplaceAll(exporter.query, "{start}", start.Format("2006-01-02 15:04:05"))
	exporter.query = strings.ReplaceAll(exporter.query, "{end}", end.Format("2006-01-02 15:04:05"))
	exporter.query = strings.ReplaceAll(exporter.query, "{tag}", "%%")

	rows, err := exporter.Db.Query(exporter.query)
	if err != nil {
		return nil, fmt.Errorf("ошибка загрузки данных из базы данных: %s", err)
	}
	defer rows.Close()

	data := make([]map[string]interface{}, 0)

	// if app.outputFormat == "csv" && app.csvHeader {
	// 	data = append(data, app.writeHeader(rows))
	// }

	for rows.Next() {
		data = append(data, exporter.writeRow(rows))
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("нет данных для обработки")
	}

	if !exporter.silient {
		fmt.Printf("- %d строк за %s\n", len(data), time.Since(beg).Truncate(time.Second))
	}
	return data, nil
}

// сохраняет данные в файл
func (exporter *Exporter) saveData(start time.Time, end time.Time, data []map[string]interface{}) error {
	beg := time.Now()
	if !exporter.silient {
		fmt.Print("Сохранение данных в файл")
	}
	fileName := exporter.generateFileName(start, end)
	outputPath := filepath.Dir(fileName)
	// проверяем путь к выходному файлу и создаем его, если не существует
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		err := os.MkdirAll(outputPath, 0755)
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
	switch exporter.compression {
	case "gz":
		writer = gzip.NewWriter(file)
		defer writer.(*gzip.Writer).Close()
	case "lz4":
		writer = lz4.NewWriter(file)
		defer writer.(*lz4.Writer).Close()
	default:
		writer = file
	}

	switch exporter.outputFormat {
	case "csv":
		encoder := csv.NewWriter(writer)
		// первый символ из app.csvDelimiter
		encoder.Comma = rune(exporter.csvDelimiter[0])
		encoder.WriteAll(exporter.convertDataToCsv(data))
		encoder.Flush()
	case "json":
		encoder := json.NewEncoder(writer)
		encoder.Encode(data)
	case "yaml":
		encoder := yaml.NewEncoder(writer)
		encoder.Encode(data)
	// todo: не реализовано
	// case "toml":
	// 	encoder := toml.NewEncoder(writer)
	// 	encoder.Encode(data)
	default:
		return fmt.Errorf("неизвестный формат выходных данных: %s", exporter.outputFormat)
	}

	err = exporter.saveLastPeriodDate(end)
	if err != nil {
		return err
	}

	if !exporter.silient {
		fmt.Printf(" - %s\n", time.Since(beg).Truncate(time.Second))
	}

	return nil
}

// записывает строку в массив данных
func (exporter *Exporter) writeRow(rows *sql.Rows) map[string]interface{} {
	var err error
	exporter.columns, err = rows.Columns()
	if err != nil {
		panic(fmt.Errorf("ошибка получения столбцов: %s", err))
	}

	values := make([]interface{}, len(exporter.columns))
	valuePtrs := make([]interface{}, len(exporter.columns))
	for i := range exporter.columns {
		valuePtrs[i] = &values[i]
	}

	err = rows.Scan(valuePtrs...)
	if err != nil {
		panic(fmt.Errorf("ошибка чтения строк: %s", err))
	}

	row := make(map[string]interface{})
	for i, col := range exporter.columns {
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

	return row
}

// конвертирует данные из базы данных в формат CSV
func (exporter *Exporter) convertDataToCsv(data []map[string]interface{}) [][]string {
	rows := make([][]string, len(data))

	if exporter.csvHeader {
		rows[0] = exporter.columns
		rows = append(rows, []string{})
	}

	for i, d := range data {
		row := []string{}
		// must be alvays in same order
		for _, k := range exporter.columns {
			row = append(row, fmt.Sprintf("%v", d[k]))
		}
		if !exporter.csvHeader {
			rows[i] = row
		} else {
			rows[i+1] = row
		}
	}

	return rows
}
