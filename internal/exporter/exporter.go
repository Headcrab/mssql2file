package exporter

import (
	"database/sql"
	"mssql2file/internal/errors"

	"encoding/json"
	"fmt"

	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"

	"mssql2file/internal/compressor"
	"mssql2file/internal/config"
	"mssql2file/internal/format"
)

// структура, представляющая приложение
type Exporter struct {
	Db     *sql.DB // источник данных
	config *config.Config
	isLast bool
	start  time.Time     // начальная дата и время
	period time.Duration // длительность периода

}

// создает новое приложение с заданными параметрами командной строки
func Create(args *config.Config) (*Exporter, error) {

	app := &Exporter{
		config: args,
	}

	if args.Last_period_end == "" && args.Start == "last" {
		return nil, errors.New(errors.BeginDateNotSet, "")
	}
	var err error
	app.isLast = false
	if args.Start == "last" {
		app.start, err = time.Parse("2006-01-02 15:04:05", args.Last_period_end)
		if err != nil {
			return nil, errors.New(errors.BeginDateParse, err.Error())
		}
		app.isLast = true
	} else {

		app.start, err = time.Parse("2006-01-02 15:04:05", args.Start)
		if err != nil {
			return nil, errors.New(errors.BeginDateParse, err.Error())
		}
	}

	app.period, err = time.ParseDuration(args.Period)
	if err != nil {
		return nil, errors.New(errors.PeriodParse, err.Error())
	}
	if app.period > 24*time.Hour {
		return nil, errors.New(errors.PeriodTooLong, "")
	}

	return app, nil
}

// генерирует имя файла для выходного файла
func (exporter *Exporter) generateFileName(start time.Time, end time.Time) string {
	if exporter.config.Output != "" && exporter.config.Output[len(exporter.config.Output)-1:] != "/" {
		exporter.config.Output += "/"
	}
	fileName := exporter.config.Output + exporter.config.Template
	fileName = strings.ReplaceAll(fileName, "{period}", exporter.period.String())
	fileName = strings.ReplaceAll(fileName, "{start}", start.Format(exporter.config.Date_format))
	fileName = strings.ReplaceAll(fileName, "{end}", end.Format(exporter.config.Date_format))
	fileName = strings.ReplaceAll(fileName, "{format}", exporter.config.Output_format)
	if exporter.config.Compression == "none" {
		fileName = strings.ReplaceAll(fileName, ".{compression}", "")
	} else {
		fileName = strings.ReplaceAll(fileName, "{compression}", exporter.config.Compression)
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

	if !exporter.config.Silient {
		// время выполнения программы в формате 1h2m3s
		fmt.Println("Время обработки: ", time.Since(progStart).Truncate(time.Second))
	}

	return nil
}

// подключается к базе данных
func (exporter *Exporter) connectToDatabase() error {
	var err error
	exporter.Db, err = sql.Open("mssql", exporter.config.Connection_string)
	if err != nil {
		return errors.New(errors.DbConnection, err.Error())
	}
	return nil
}

// сохраняет дату последнего обработанного периода в файл
func (exporter *Exporter) saveLastPeriodDate(end time.Time) error {
	// получаем путь к выходному файлу
	outputPath := filepath.Dir(exporter.config.Config_file)
	var file *os.File
	var config map[string]interface{}
	var err error
	if _, e := os.Stat(exporter.config.Config_file); os.IsNotExist(e) {
		// если файла не существует, то создаем его и пишем в него дату последнего обработанного периода app.lastPeriodDate
		file, err = exporter.createNewFile(outputPath)
	} else {
		// если файл существует, то пишем в него дату последнего обработанного периода
		file, err = exporter.getExistingFile(&config)
	}
	if err != nil {
		return err
	}
	defer file.Close()
	// добавляем в структуру новую дату последнего обработанного периода
	if config == nil {
		config = make(map[string]interface{})
	}
	// пишем в файл дату последнего обработанного периода в json 'Last_period_end'
	config["Last_period_end"] = end.Format("2006-01-02 15:04:05")
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(config)
	if err != nil {
		return errors.New(errors.LastPeriodWrite, err.Error())
	}

	return nil
}

func (exporter *Exporter) getExistingFile(config *map[string]interface{}) (*os.File, error) {
	file, err := os.Open(exporter.config.Config_file)
	if err != nil {
		return nil, errors.New(errors.LastPeriodRead, err.Error())
	}
	defer file.Close()

	err = json.NewDecoder(file).Decode(&config)
	if err != nil {
		return nil, errors.New(errors.LastPeriodParse, err.Error())
	}

	file.Close()

	file, err = os.OpenFile(exporter.config.Config_file, os.O_RDWR, 0755)
	if err != nil {
		return nil, errors.New(errors.LastPeriodFileOpen, err.Error())
	}
	return file, nil
}

func (exporter *Exporter) createNewFile(outputPath string) (*os.File, error) {
	err := os.MkdirAll(outputPath, 0755)
	if err != nil {
		return nil, errors.New(errors.LastPeriodFolderCreate, err.Error())
	}

	file, err := os.Create(exporter.config.Config_file)
	if err != nil {
		return nil, errors.New(errors.LastPeriodFileCreate, err.Error())
	}
	return file, nil
}

// обрабатывает все периоды
func (exporter *Exporter) processAllPeriods(start time.Time) error {
	now, _ := time.Parse("2006-01-02 15:04:05", time.Now().Format("2006-01-02 15:04:05"))
	if exporter.config.Count == 0 {
		// рассчитываем количество периодов с учетом периода и даты начала и текущего момента
		exporter.config.Count = int(now.Sub(start).Minutes() / exporter.period.Minutes())
	}
	for i := 0; i < exporter.config.Count; i++ {
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

	if !exporter.config.Silient {
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
	if !exporter.config.Silient {
		fmt.Print("Загрузка данных из базы данных ")
	}
	exporter.config.Query = strings.ReplaceAll(exporter.config.Query, "{start}", start.Format("2006-01-02 15:04:05"))
	exporter.config.Query = strings.ReplaceAll(exporter.config.Query, "{end}", end.Format("2006-01-02 15:04:05"))
	exporter.config.Query = strings.ReplaceAll(exporter.config.Query, "{tag}", "%%")

	rows, err := exporter.Db.Query(exporter.config.Query)
	if err != nil {
		return nil, errors.New(errors.DbQuery, err.Error())
	}
	defer rows.Close()

	data := make([]map[string]interface{}, 0)

	for rows.Next() {
		d, err := exporter.writeRow(rows)
		if err != nil {
			return nil, err
		}
		data = append(data, d)
	}

	if len(data) == 0 {
		return nil, errors.New(errors.DbNoData, "")
	}

	if !exporter.config.Silient {
		fmt.Printf("- %d строк за %s\n", len(data), time.Since(beg).Truncate(time.Second))
	}
	return data, nil
}

// сохраняет данные в файл
func (exporter *Exporter) saveData(start time.Time, end time.Time, data []map[string]interface{}) error {
	beg := time.Now()
	if !exporter.config.Silient {
		fmt.Print("Сохранение данных в файл")
	}
	fileName := exporter.generateFileName(start, end)
	outputPath := filepath.Dir(fileName)
	// проверяем путь к выходному файлу и создаем его, если не существует
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		err := os.MkdirAll(outputPath, 0755)
		if err != nil {
			return errors.New(errors.OutputWrongPath, err.Error())
		}
	}

	file, err := os.Create(fileName)
	if err != nil {
		return errors.New(errors.OutputCreateFile, err.Error())
	}
	defer file.Close()

	compressor, err := compressor.NewCompressor(exporter.config.Compression, file)
	if err != nil {
		return err
	}
	defer compressor.Close()

	encoder, err := format.NewEncoder(exporter.config.Output_format, compressor)
	if err != nil {
		return err
	}
	encoder.SetFormatParams(exporter.getFormatParams())
	encoder.Encode(data)

	err = exporter.saveLastPeriodDate(end)
	if err != nil {
		return err
	}

	if !exporter.config.Silient {
		fmt.Printf(" - %s\n", time.Since(beg).Truncate(time.Second))
	}

	return nil
}

// записывает строку в массив данных
func (exporter *Exporter) writeRow(rows *sql.Rows) (map[string]interface{}, error) {
	var err error
	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.New(errors.DbColumns, err.Error())
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	err = rows.Scan(valuePtrs...)
	if err != nil {
		return nil, errors.New(errors.DbScan, err.Error())
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

	return row, nil
}

func (exporter *Exporter) getFormatParams() map[string]interface{} {
	params := make(map[string]interface{})
	params["delimiter"] = exporter.config.Csv_delimiter
	params["header"] = exporter.config.Csv_header
	return params
}
