package exporter

import (
	"database/sql"
<<<<<<< HEAD
	"mssql2file/internal/apperrors"

	// "sync" // for v2
=======
>>>>>>> e7725ee (+ config, format, comressor, exported moved)

	"encoding/json"
	"fmt"

	"os"
	"path/filepath"
	"strings"
	"time"

<<<<<<< HEAD
	"mssql2file/internal/compressor"
	"mssql2file/internal/config"
	"mssql2file/internal/format"

	// mssql
	_ "github.com/denisenkom/go-mssqldb"
	// mysql
	_ "github.com/go-sql-driver/mysql"
	// clickhouse
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"golang.org/x/text/encoding/charmap"
=======
	_ "github.com/denisenkom/go-mssqldb"

	"mssql2file/internal/compressors"
	"mssql2file/internal/configs"
	"mssql2file/internal/formats"
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
)

// структура, представляющая приложение
type Exporter struct {
	Db     *sql.DB // источник данных
<<<<<<< HEAD
	config *config.Config
=======
	config *configs.Config
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
	isLast bool
	start  time.Time     // начальная дата и время
	period time.Duration // длительность периода

}

// создает новое приложение с заданными параметрами командной строки
<<<<<<< HEAD
func Create(args *config.Config) (*Exporter, error) {
=======
func NewExporter(args *configs.Config) (*Exporter, error) {
>>>>>>> e7725ee (+ config, format, comressor, exported moved)

	app := &Exporter{
		config: args,
	}

	if args.Last_period_end == "" && args.Start == "last" {
<<<<<<< HEAD
		return nil, apperrors.New(apperrors.BeginDateNotSet, "")
=======
		return nil, fmt.Errorf("не задана дата начала обработки")
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
	}
	var err error
	app.isLast = false
	if args.Start == "last" {
		app.start, err = time.Parse("2006-01-02 15:04:05", args.Last_period_end)
		if err != nil {
<<<<<<< HEAD
			return nil, apperrors.New(apperrors.BeginDateParse, err.Error())
=======
			return nil, fmt.Errorf("ошибка при разборе даты: %v", err)
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
		}
		app.isLast = true
	} else {

		app.start, err = time.Parse("2006-01-02 15:04:05", args.Start)
		if err != nil {
<<<<<<< HEAD
			return nil, apperrors.New(apperrors.BeginDateParse, err.Error())
=======
			return nil, fmt.Errorf("ошибка при разборе даты: %v", err)
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
		}
	}

	app.period, err = time.ParseDuration(args.Period)
	if err != nil {
<<<<<<< HEAD
		return nil, apperrors.New(apperrors.PeriodParse, err.Error())
	}
	if app.period > 24*time.Hour {
		return nil, apperrors.New(apperrors.PeriodTooLong, "")
=======
		return nil, fmt.Errorf("ошибка при разборе периода: %v", err)
	}
	if app.period > 24*time.Hour {
		return nil, fmt.Errorf("период не может быть больше 24 часов")
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
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
<<<<<<< HEAD
=======

>>>>>>> e7725ee (+ config, format, comressor, exported moved)
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
<<<<<<< HEAD
	dbtype := exporter.config.Connection_type
	// todo: хзхз
	if dbtype == "" {
		dbtype = "mssql"
	}
	exporter.Db, err = sql.Open(dbtype, exporter.config.Connection_string)
	if err != nil {
		return apperrors.New(apperrors.DbConnection, err.Error())
	}
	err = exporter.Db.Ping()
	if err != nil {
		return err
	}

	return nil
}

// Сохраняет дату последнего обработанного периода в файл
func (exporter *Exporter) saveLastPeriodDate(end time.Time) error {
	// Получаем путь к выходному файлу.
	outputPath := filepath.Dir(exporter.config.Config_file)

	// Проверяем, существует ли файл, и читаем его содержимое в config, если он существует.
	config := make(map[string]interface{})
	if _, e := os.Stat(exporter.config.Config_file); !os.IsNotExist(e) {
		if file, err := os.Open(exporter.config.Config_file); err == nil {
			defer file.Close()
			if err = json.NewDecoder(file).Decode(&config); err != nil {
				return apperrors.New(apperrors.LastPeriodWrite, err.Error())
			}
		} else {
			return err
		}
	}

	// Обновляем config новой датой.
	config["Last_period_end"] = end.Format("2006-01-02 15:04:05")

	// Записываем обновленный config в файл.
	if file, err := exporter.createNewFile(outputPath); err == nil {
		defer file.Close()
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		if err = encoder.Encode(config); err != nil {
			return apperrors.New(apperrors.LastPeriodWrite, err.Error())
		}
	} else {
		return err
=======
	exporter.Db, err = sql.Open("mssql", exporter.config.Connection_string)
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
	outputPath := filepath.Dir(exporter.config.Config_file)
	var file *os.File
	var config map[string]interface{}
	// проверяем существование файла и создаем его, если не существует
	if _, err := os.Stat(exporter.config.Config_file); os.IsNotExist(err) {
		// проверяем существование папки
		if _, err := os.Stat(outputPath); os.IsNotExist(err) {
			// создаем папку
			err = os.MkdirAll(outputPath, 0755)
			if err != nil {
				return fmt.Errorf("ошибка создания папки для файла последнего обработанного периода: %s", err)
			}
		}
		// создаем файл
		file, err = os.Create(exporter.config.Config_file)
		if err != nil {
			return fmt.Errorf("ошибка создания файла последнего обработанного периода: %s", err)
		}
		defer file.Close()
	} else {
		// читаем существующие данные из файла
		file, err = os.Open(exporter.config.Config_file)
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
		file, err = os.OpenFile(exporter.config.Config_file, os.O_RDWR, 0755)
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
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
	}

	return nil
}

<<<<<<< HEAD
func (exporter *Exporter) createNewFile(outputPath string) (*os.File, error) {
	err := os.MkdirAll(outputPath, 0755)
	if err != nil {
		return nil, apperrors.New(apperrors.LastPeriodFolderCreate, err.Error())
	}

	file, err := os.Create(exporter.config.Config_file)
	if err != nil {
		return nil, apperrors.New(apperrors.LastPeriodFileCreate, err.Error())
	}
	return file, nil
}

=======
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
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
<<<<<<< HEAD
			if !exporter.config.Silient {
				fmt.Println(err)
			}
=======
			return err
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
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
<<<<<<< HEAD
func (exporter *Exporter) loadData(start time.Time, end time.Time) (*[]map[string]string, error) {
=======
func (exporter *Exporter) loadData(start time.Time, end time.Time) ([]map[string]interface{}, error) {
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
	beg := time.Now()
	if !exporter.config.Silient {
		fmt.Print("Загрузка данных из базы данных ")
	}
<<<<<<< HEAD
	query := strings.ReplaceAll(exporter.config.Query, "{start}", start.Format("2006-01-02 15:04:05"))
	query = strings.ReplaceAll(query, "{end}", end.Format("2006-01-02 15:04:05"))
	query = strings.ReplaceAll(query, "{tag}", "%%")

	rows, err := exporter.Db.Query(query)
	if err != nil {
		return nil, apperrors.New(apperrors.DbQuery, err.Error())
	}
	defer rows.Close()

	data := make([]map[string]string, 0, 100000)

	// fix: v1
	for rows.Next() {
		d, err := exporter.writeRow(rows)
		if err != nil {
			return nil, err
		}
		if exporter.config.Decoder != "" {
			var enc *charmap.Charmap
			switch exporter.config.Decoder {
			case "windows-1251":
				enc = charmap.Windows1251
			case "koi8-r":
				enc = charmap.KOI8R
			default:
				enc = charmap.Windows1251
			}
			for k, v := range d {
				if v != "" {
					d[k], _ = enc.NewDecoder().String(v)
				}
			}
		}
		data = append(data, d)
	}

	// fix: v2

	// dataChannel := make(chan map[string]interface{}, 1000) // Буферизованный канал
	// errChannel := make(chan error, 1)                      // Канал для ошибок
	// const numWorkers = 10                                  // количество рабочих goroutines

	// var wg sync.WaitGroup
	// var dataMutex sync.Mutex

	// // Горутина для считывания из базы данных
	// go func() {
	// 	for rows.Next() {
	// 		rowData, err := exporter.writeRow(rows)
	// 		if err != nil {
	// 			errChannel <- err
	// 			return
	// 		}
	// 		// fmt.Println("dataChannel <- rowData")
	// 		dataChannel <- rowData
	// 	}
	// 	close(dataChannel)
	// }()

	// // Рабочие горутины
	// for i := 0; i < numWorkers; i++ {
	// 	// fmt.Println("wg.Add(1)")
	// 	wg.Add(1)
	// 	go func() {
	// 		defer wg.Done()
	// 		for d := range dataChannel {
	// 			// fmt.Println("data = append(data, d)")
	// 			// тут обработка данных (если требуется)
	// 			dataMutex.Lock()
	// 			data = append(data, d)
	// 			dataMutex.Unlock()
	// 		}
	// 	}()
	// }

	// // Ждём завершения всех рабочих горутин
	// wg.Wait()

	// // Проверка наличия ошибок
	// select {
	// case err := <-errChannel:
	// 	if err != nil {
	// 		// fmt.Println("Error:", err)
	// 		return nil, err
	// 	}
	// default:
	// 	// Нет ошибок
	// }
	// v2

	if len(data) == 0 {
		return nil, apperrors.New(apperrors.DbNoData, "")
=======
	exporter.config.Query = strings.ReplaceAll(exporter.config.Query, "{start}", start.Format("2006-01-02 15:04:05"))
	exporter.config.Query = strings.ReplaceAll(exporter.config.Query, "{end}", end.Format("2006-01-02 15:04:05"))
	exporter.config.Query = strings.ReplaceAll(exporter.config.Query, "{tag}", "%%")

	rows, err := exporter.Db.Query(exporter.config.Query)
	if err != nil {
		return nil, fmt.Errorf("ошибка загрузки данных из базы данных: %s", err)
	}
	defer rows.Close()

	data := make([]map[string]interface{}, 0)

	for rows.Next() {
		data = append(data, exporter.writeRow(rows))
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("нет данных для обработки")
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
	}

	if !exporter.config.Silient {
		fmt.Printf("- %d строк за %s\n", len(data), time.Since(beg).Truncate(time.Second))
	}
<<<<<<< HEAD
	return &data, nil
}

// сохраняет данные в файл
func (exporter *Exporter) saveData(start time.Time, end time.Time, data *[]map[string]string) error {
=======
	return data, nil
}

// сохраняет данные в файл
func (exporter *Exporter) saveData(start time.Time, end time.Time, data []map[string]interface{}) error {
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
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
<<<<<<< HEAD
			return apperrors.New(apperrors.OutputWrongPath, err.Error())
=======
			return fmt.Errorf("неверный путь к выходному файлу: %s", err)
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
		}
	}

	file, err := os.Create(fileName)
	if err != nil {
<<<<<<< HEAD
		return apperrors.New(apperrors.OutputCreateFile, err.Error())
	}
	defer file.Close()

	compressor, err := compressor.NewCompressor(exporter.config.Compression, file)
=======
		return fmt.Errorf("ошибка создания файла: %s", err)
	}
	defer file.Close()

	compressor, err := compressors.NewCompressor(exporter.config.Compression, file)
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
	if err != nil {
		return err
	}
	defer compressor.Close()

<<<<<<< HEAD
	encoder, err := format.NewEncoder(exporter.config.Output_format, compressor)
=======
	encoder, err := formats.NewEncoder(exporter.config.Output_format, compressor)
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
	if err != nil {
		return err
	}
	encoder.SetFormatParams(exporter.getFormatParams())
<<<<<<< HEAD
	encoder.Encode(*data)
=======
	encoder.Encode(data)
>>>>>>> e7725ee (+ config, format, comressor, exported moved)

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
<<<<<<< HEAD
func (exporter *Exporter) writeRow(rows *sql.Rows) (map[string]string, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
=======
func (exporter *Exporter) writeRow(rows *sql.Rows) map[string]interface{} {
	var err error
	columns, err := rows.Columns()
	if err != nil {
		panic(fmt.Errorf("ошибка получения столбцов: %s", err))
>>>>>>> e7725ee (+ config, format, comressor, exported moved)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

<<<<<<< HEAD
	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, fmt.Errorf("failed to scan row values: %w", err)
	}

	row := make(map[string]string, len(columns))
	for i, col := range columns {
		if values[i] == nil {
			row[col] = ""
			continue
		}
		val := values[i]
		switch v := val.(type) {
		case []byte:
			row[col] = string(v)
		default:
			row[col] = fmt.Sprintf("%v", v)
		}
	}

	return row, nil
}

// записывает строку в массив данных
=======
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

	return row
}

>>>>>>> e7725ee (+ config, format, comressor, exported moved)
func (exporter *Exporter) getFormatParams() map[string]interface{} {
	params := make(map[string]interface{})
	params["delimiter"] = exporter.config.Csv_delimiter
	params["header"] = exporter.config.Csv_header
	return params
}
