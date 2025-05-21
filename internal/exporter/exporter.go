package exporter

import (
	"database/sql"
	"fmt" // Ensure fmt is imported
	"mssql2file/internal/apperrors"

	"sync" // for v2

	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/text/transform"

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

	slog.Info("Total processing time", "duration", time.Since(progStart).Truncate(time.Second))

	return nil
}

// подключается к базе данных
func (exporter *Exporter) connectToDatabase() error {
	var err error
	// exporter.config.Connection_type is guaranteed to be non-empty by config.Load(),
	// defaulting to "mssql" if not specified by user.
	dbtype := exporter.config.Connection_type
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
				return fmt.Errorf("failed to decode config file: %w", err)
			}
		} else {
			return fmt.Errorf("failed to open config file: %w", err)
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
			return fmt.Errorf("failed to encode config to file: %w", err)
		}
	} else {
		return fmt.Errorf("failed to create new file: %w", err)
	}

	return nil
}

func (exporter *Exporter) createNewFile(outputPath string) (*os.File, error) {
	err := os.MkdirAll(outputPath, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create output path: %w", err)
	}

	file, err := os.Create(exporter.config.Config_file)
	if err != nil {
		return nil, fmt.Errorf("failed to create config file: %w", err)
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
			// Return the first error encountered
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

	slog.Info("Processing period", "start_time", start.Format("2006-01-02 15:04:05"), "end_time", end.Format("2006-01-02 15:04:05"))

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
	slog.Debug("Loading data from database", "start_time", start, "end_time", end)

	// Prepare query with placeholders
	// Assuming the original query in config is like:
	// "SELECT TagName, format(DateTime, 'yyyy-MM-dd HH:mm:ss.fff') as DateTime, Value FROM history WHERE DateTime > {start} AND DateTime <= {end} AND TagName like {tag} AND Value is not null;"
	// We need to replace {start}, {end}, {tag} with ?
	// This is a simplified approach; a more robust solution might involve parsing the query
	// or defining query structures more explicitly.
	query := exporter.config.Query
	query = strings.Replace(query, "{start}", "?", 1)
	query = strings.Replace(query, "{end}", "?", 1)
	query = strings.Replace(query, "{tag}", "?", 1)
	// Additional placeholders if any would need similar treatment or a more generic substitution.

	rows, err := exporter.Db.Query(query, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"), "%%")
	if err != nil {
		return nil, fmt.Errorf("failed to execute parameterized query: %w", err)
	}
	defer rows.Close()

	data := make([]map[string]string, 0, 100000)

	var decoder transform.Transformer
	if exporter.config.Decoder != "" {
		var enc *charmap.Charmap
		switch exporter.config.Decoder {
		case "windows-1251":
			enc = charmap.Windows1251
		case "koi8-r":
			enc = charmap.KOI8R
		default:
			slog.Warn("Unsupported decoder specified, defaulting to windows-1251", "decoder", exporter.config.Decoder)
			enc = charmap.Windows1251 // Default to windows-1251 if unsupported type
		}
		if enc != nil {
			decoder = enc.NewDecoder()
		}
	}

	// Commenting out the entire v1 row processing loop.
	// for rows.Next() {
	// 	d, err := exporter.writeRow(rows)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	if decoder != nil {
	// 		for k, v := range d {
	// 			if v != "" {
	// 				// transformer.String can return an error, which should be handled.
	// 				decodedValue, _, err := transform.String(decoder, v)
	// 				if err != nil {
	// 					return nil, fmt.Errorf("failed to decode value for key %s (original value: '%s'): %w", k, v, err)
	// 				}
	// 				d[k] = decodedValue
	// 			}
	// 		}
	// 	}
	// 	// data = append(data, d) // This line is part of v1, will be commented out
	// }
	// End of v1 row processing loop

	// fix: v2 (activated)
	dataChannel := make(chan map[string]string, 1000) // Буферизованный канал, type changed to map[string]string
	errChannel := make(chan error, 1)                 // Канал для ошибок
	const numWorkers = 10                             // количество рабочих goroutines

	var wg sync.WaitGroup
	var dataMutex sync.Mutex

	// Горутина для считывания из базы данных
	go func() {
		defer close(dataChannel) // Close dataChannel when rows.Next() is done
		for rows.Next() {
			rowData, err := exporter.writeRow(rows)
			if err != nil {
				select {
				case errChannel <- err:
				default: // Avoid blocking if errChannel is full or already has an error
				}
				return
			}
			dataChannel <- rowData
		}
	}()

	// Рабочие горутины
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range dataChannel {
				if decoder != nil {
					for k, v := range d {
						if v != "" {
							decodedValue, _, err := transform.String(decoder, v)
							if err != nil {
								select {
								case errChannel <- fmt.Errorf("failed to decode value for key %s (original value: '%s'): %w", k, v, err):
								default:
								}
								return // Stop this worker on decoding error
							}
							d[k] = decodedValue
						}
					}
				}
				dataMutex.Lock()
				data = append(data, d)
				dataMutex.Unlock()
			}
		}()
	}

	// Ждём завершения всех рабочих горутин
	wg.Wait()

	// Проверка наличия ошибок
	// Closing errChannel is not strictly necessary here as we only read one error or none.
	select {
	case err := <-errChannel:
		if err != nil {
			return nil, err // Return the first error encountered
		}
	default:
		// Нет ошибок
	}
	// v2 end

	if len(data) == 0 {
		// Return a specific error if no data is found
		return nil, apperrors.New(apperrors.DbNoData, "")
	}

	slog.Info("Data loaded", "rows_count", len(data), "duration", time.Since(beg).Truncate(time.Second))
	return &data, nil
}

// сохраняет данные в файл
func (exporter *Exporter) saveData(start time.Time, end time.Time, data *[]map[string]string) error {
	beg := time.Now()
	fileName := exporter.generateFileName(start, end)
	slog.Debug("Saving data to file", "filename", fileName)
	outputPath := filepath.Dir(fileName)
	// проверяем путь к выходному файлу и создаем его, если не существует
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		err := os.MkdirAll(outputPath, 0755)
		if err != nil {
			return fmt.Errorf("failed to create output path: %w", err)
		}
	}

	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
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

	slog.Info("Data saved", "filename", fileName, "duration", time.Since(beg).Truncate(time.Second))

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
