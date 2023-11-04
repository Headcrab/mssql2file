package exporter

import (
	"database/sql"
	"mssql2file/internal/apperrors"

	// "sync" // for v2

	"encoding/json"
	"fmt"

	"os"
	"path/filepath"
	"strings"
	"time"

	"mssql2file/internal/compressor"
	"mssql2file/internal/config"
	"mssql2file/internal/format"

	// mssql
	_ "github.com/denisenkom/go-mssqldb"
	// mysql
	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/text/encoding/charmap"
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
		return nil, apperrors.New(apperrors.BeginDateNotSet, "")
	}
	var err error
	app.isLast = false
	if args.Start == "last" {
		app.start, err = time.Parse("2006-01-02 15:04:05", args.Last_period_end)
		if err != nil {
			return nil, apperrors.New(apperrors.BeginDateParse, err.Error())
		}
		app.isLast = true
	} else {

		app.start, err = time.Parse("2006-01-02 15:04:05", args.Start)
		if err != nil {
			return nil, apperrors.New(apperrors.BeginDateParse, err.Error())
		}
	}

	app.period, err = time.ParseDuration(args.Period)
	if err != nil {
		return nil, apperrors.New(apperrors.PeriodParse, err.Error())
	}
	if app.period > 24*time.Hour {
		return nil, apperrors.New(apperrors.PeriodTooLong, "")
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
	}

	return nil
}

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
			if !exporter.config.Silient {
				fmt.Println(err)
			}
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
func (exporter *Exporter) loadData(start time.Time, end time.Time) (*[]map[string]string, error) {
	beg := time.Now()
	if !exporter.config.Silient {
		fmt.Print("Загрузка данных из базы данных ")
	}
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
	}

	if !exporter.config.Silient {
		fmt.Printf("- %d строк за %s\n", len(data), time.Since(beg).Truncate(time.Second))
	}
	return &data, nil
}

// сохраняет данные в файл
func (exporter *Exporter) saveData(start time.Time, end time.Time, data *[]map[string]string) error {
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
			return apperrors.New(apperrors.OutputWrongPath, err.Error())
		}
	}

	file, err := os.Create(fileName)
	if err != nil {
		return apperrors.New(apperrors.OutputCreateFile, err.Error())
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
	encoder.Encode(*data)

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
func (exporter *Exporter) writeRow(rows *sql.Rows) (map[string]string, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

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
func (exporter *Exporter) getFormatParams() map[string]interface{} {
	params := make(map[string]interface{})
	params["delimiter"] = exporter.config.Csv_delimiter
	params["header"] = exporter.config.Csv_header
	return params
}
