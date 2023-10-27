package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"mssql2file/internal/apperrors"
	"os"
	"reflect"
	"strings"
)

// константы
const (
	defaultStart            = "last"
	defaultPeriod           = "1m"
	defaultOutput           = "."
	defaultTemplate         = "hs_{start}_{end}_{period}.{format}.{compression}"
	defaultCount            = 0
	defaultOutputFormat     = "json"
	defaultCsvDelimiter     = ";"
	defaultCsvHeader        = false
	defaultCompression      = "gz"
	defaultDateFormat       = "060102_150405"
	defaultConnectionString = "server=139.158.31.1;port=1433;user id=sa;password=!QAZ1qaz12345;database=runtime;TrustServerCertificate=true;encrypt=disable;connection timeout=1000;"
	defaultQuery            = "SELECT TagName, format(DateTime, 'yyyy-MM-dd HH:mm:ss.fff') as DateTime, Value FROM history WHERE DateTime > '{start}' AND DateTime <= '{end}' AND TagName like '{tag}' AND Value is not null;"
	defaultConfigFile       = "mssql2file.cfg"
	defaultLastPeriodEnd    = ""
	envVarPrefix            = "M2F"
)

// структура, представляющая параметры командной строки
type Config struct {
	Help              bool   // флаг, указывающий, что нужно вывести справку по параметрам командной строки
	Silient           bool   // флаг, указывающий, что не нужно выводить сообщения в консоль
	Start             string // начальная дата и время (формат: '2006-01-02 15:04:05' или 'last'), по умолчанию: last
	Period            string // длительность периода (формат: 1h, 5m и т.д.) (не более 24 часов), по умолчанию: 1m
	Output            string // директория для сохранения выходных файлов, по умолчанию: текущая директория
	Template          string // шаблон имени выходных файлов, по умолчанию: hs_{start}_{end}_{period}.{format}.{compression}
	Count             int    // количество периодов для обработки, 0 - обработать все периоды до текущего момента, по умолчанию: 0
	Output_format     string // формат выходных файлов (json, csv, xml, yaml, toml), по умолчанию: json
	Csv_delimiter     string // разделитель полей в csv-файле, по умолчанию: ;
	Csv_header        bool   // выводить заголовок в csv-файле, по умолчанию: false
	Compression       string // метод сжатия (none, gz, lz4), по умолчанию: gz
	Date_format       string // формат даты для использования в имени файла, по умолчанию: 060102_150405
	Connection_string string // строка подключения к БД MSSQL, по умолчанию HS0
	Query             string // запрос к БД MSSQL, по умолчанию: SELECT TagName, format(DateTime, 'yyyy-MM-dd HH:mm:ss.fff') as DateTime, Value FROM history WHERE DateTime > '{start}' AND DateTime <= '{end}' AND TagName like '{tag}' AND Value is not null;
	Config_file       string // файл конфигурации, по умолчанию: mssql2file.cfg
	Last_period_end   string // дата и время окончания последнего обработанного периода, по умолчанию: ''

	printAppNameFunc func()
}

// стандартные значения
var defaultArgs = Config{
	Silient:           false,
	Start:             defaultStart,
	Period:            defaultPeriod,
	Output:            defaultOutput,
	Template:          defaultTemplate,
	Count:             defaultCount,
	Output_format:     defaultOutputFormat,
	Csv_delimiter:     defaultCsvDelimiter,
	Csv_header:        defaultCsvHeader,
	Compression:       defaultCompression,
	Date_format:       defaultDateFormat,
	Connection_string: defaultConnectionString,
	Query:             defaultQuery,
	Config_file:       defaultConfigFile,
	Last_period_end:   defaultLastPeriodEnd,
}

func New() *Config {
	return &Config{}
}

// Загрузка параметров командной строки и возвращает структуру Config
func (args *Config) Load() error {

	flag.BoolVar(&args.Silient, "silient", false, "флаг, указывающий, что не нужно выводить сообщения в консоль")
	flag.StringVar(&args.Start, "start", "", "начальная дата и время (формат: '2006-01-02 15:04:05' или 'last'), по умолчанию: last")
	flag.StringVar(&args.Period, "period", "", "длительность периода (формат: 1h, 5m и т.д.) (не более 24 часов), по умолчанию: 1m")
	flag.StringVar(&args.Output, "output", "", "директория для сохранения выходных файлов, по умолчанию: текущая директория")
	flag.StringVar(&args.Template, "name", "", "шаблон имени выходных файлов, по умолчанию: hs_{start}_{end}_{period}.{format}.{compression}")
	flag.IntVar(&args.Count, "count", 0, "количество периодов для обработки, 0 - обработать все периоды до текущего момента, по умолчанию: 0")
	flag.StringVar(&args.Output_format, "format", "", "формат выходных файлов (json, csv, xml, yaml, toml), по умолчанию: json")
	flag.StringVar(&args.Csv_delimiter, "csv_delimiter", "", "разделитель полей в csv-файле, по умолчанию: ;")
	flag.BoolVar(&args.Csv_header, "csv_header", false, "выводить заголовок в csv-файле, по умолчанию: false")
	flag.StringVar(&args.Compression, "compression", "", "метод сжатия (none, gz, lz4), по умолчанию: gz")
	flag.StringVar(&args.Date_format, "date_format", "", "формат даты для использования в имени файла, по умолчанию: 060102_150405")
	flag.StringVar(&args.Connection_string, "connection_string", "", "строка подключения к БД MSSQL, по умолчанию HS0")
	flag.StringVar(&args.Query, "query", "", "запрос к БД MSSQL")
	flag.StringVar(&args.Config_file, "config", "", "файл конфигурации, по умолчанию: mssql2file.cfg")
	flag.StringVar(&args.Last_period_end, "last_period_end", "", "дата и время окончания последнего периода, по умолчанию: не используется")
	help := flag.Bool("h", false, "help")
	flag.Parse()

	if *help {
		if args.printAppNameFunc != nil {
			args.printAppNameFunc()
		}
		fmt.Println("Usage:")
		flag.PrintDefaults()
		return apperrors.New(apperrors.CommandLineHelp, "-h")
	}

	err := mergeArgs(args)
	if err != nil {
		return err
	}

	return nil
}

// объединение параметров командной строки, переменных окружения, и значения конфигурации
func mergeArgs(args *Config) error {
	sources := []Config{defaultArgs, readEnvVars(envVarPrefix)}
	if defaultArgs.Config_file != "" {
		configFileArgs, err := readConfigFile(defaultArgs.Config_file)
		if err != nil {
			return err
		}
		sources = append(sources, configFileArgs)
	}
	args.add(sources...)

	return nil
}

// читает переменные окружения с префиксом prefix и возвращает структуру Config
func readEnvVars(prefix string) Config {
	v := reflect.ValueOf(&Config{}).Elem()
	t := v.Type()
	args := Config{}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		key := prefix + "_" + strings.ToUpper(field.Name)
		value := os.Getenv(key)
		if value != "" {
			v.Field(i).SetString(value)
		}
	}
	return args
}

// читает файл конфигурации и возвращает структуру Config
func readConfigFile(filePath string) (Config, error) {
	configFile, err := os.Open(filePath)
	if err != nil {
		return Config{}, err
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	args := &Config{}
	if err = jsonParser.Decode(args); err != nil {
		return Config{}, err
	}

	return *args, nil
}

// добавляет значения из source в args, если args имеет нулевое значение для поля
func (args *Config) add(sources ...Config) {
	v := reflect.ValueOf(args).Elem()
	for _, source := range sources {
		s := reflect.ValueOf(&source).Elem()
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).IsZero() {
				v.Field(i).Set(s.Field(i))
			}
		}
	}
}

func (args *Config) SetPrintFunc(printFunc func()) {
	args.printAppNameFunc = printFunc
}
