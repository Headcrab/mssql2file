package config

import (
	"encoding/json"
	// "errors"
	"flag"
	"mssql2file/internal/errors"
	"os"
	"reflect"
	"strings"
)

// структура, представляющая параметры командной строки
type Config struct {
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

// добавляет в args значения из source, если в args значение не задано
func (args *Config) add(source *Config) {
	v := reflect.ValueOf(args).Elem()
	s := reflect.ValueOf(source).Elem()
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).IsZero() {
			v.Field(i).Set(s.Field(i))
		}
	}
}

// получает параметры командной строки и возвращает структуру CommandLineArgs
func Load() (*Config, error) {
	args := &Config{}

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

	flag.Parse()

	if args.Help {
		flag.PrintDefaults()
		return nil, errors.New(errors.CommandLineHelp, "")
	}

	err := mergeArgs(args)
	if err != nil {
		return nil, err
	}

	return args, nil
}

// объединяет параметры командной строки с параметрами из файла конфигурации и переменными окружения
func mergeArgs(args *Config) error {

	gefaultArgs := Config{
		Silient:           false,
		Start:             "last",
		Period:            "1m",
		Output:            ".",
		Template:          "hs_{start}_{end}_{period}.{format}.{compression}",
		Count:             0,
		Output_format:     "json",
		Csv_delimiter:     ";",
		Csv_header:        false,
		Compression:       "gz",
		Date_format:       "060102_150405",
		Connection_string: "server=139.158.31.1;port=1433;user id=sa;password=!QAZ1qaz12345;database=runtime;TrustServerCertificate=true;encrypt=disable;connection timeout=3000;",
		Query:             "SELECT TagName, format(DateTime, 'yyyy-MM-dd HH:mm:ss.fff') as DateTime, Value FROM history WHERE DateTime > '{start}' AND DateTime <= '{end}' AND TagName like '{tag}' AND Value is not null;",
		Config_file:       "mssql2file.cfg",
		Last_period_end:   "",
	}

	// Чтение переменных окружения
	envArgs := readEnvVars("M2F")

	if args.Config_file == "" {
		args.Config_file = gefaultArgs.Config_file
	}
	configFileArgs, _ := readConfigFile(args.Config_file)

	// Объединение значений

	args.add(&configFileArgs)
	args.add(&envArgs)
	args.add(&gefaultArgs)

	return nil
}

// чтение переменных окружения с префиксом в структуру Config
func readEnvVars(prefix string) Config {
	v := reflect.ValueOf(Config{})
	t := v.Type()
	var args Config
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		key := prefix + "_" + strings.ToUpper(field.Name)
		value := os.Getenv(key)
		if value != "" {
			reflect.ValueOf(&args).Elem().Field(i).SetString(value)
		}
	}
	return args
}

// чтение файла конфигурации в структуру Config
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
