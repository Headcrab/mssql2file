package exporter

import (
	"database/sql"
	"fmt"
	"mssql2file/internal/apperrors"

	"encoding/json"
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

type queryPlaceholder struct {
	token string
	value string
}

type periodSummary struct {
	finishedAt    time.Time
	start         time.Time
	end           time.Time
	rows          int
	loadDuration  time.Duration
	saveDuration  time.Duration
	totalDuration time.Duration
	fileName      string
}

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

	err = exporter.processAllPeriods(exporter.start)
	if err != nil {
		return err
	}

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

	beg := time.Now()

	data, loadDuration, err := exporter.loadData(start, end)
	if err != nil {
		return err
	}

	fileName, saveDuration, err := exporter.saveData(start, end, data)
	if err != nil {
		return err
	}

	fmt.Fprintln(os.Stdout, formatPeriodSummary(periodSummary{
		finishedAt:    time.Now(),
		start:         start,
		end:           end,
		rows:          len(*data),
		loadDuration:  loadDuration,
		saveDuration:  saveDuration,
		totalDuration: time.Since(beg).Truncate(time.Second),
		fileName:      filepath.Base(fileName),
	}))

	return nil
}

// загружает данные из базы данных
func (exporter *Exporter) loadData(start time.Time, end time.Time) (*[]map[string]string, time.Duration, error) {
	beg := time.Now()

	query, args := prepareQuery(exporter.config.Query, start, end)
	rows, err := exporter.Db.Query(query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to execute parameterized query: %w", err)
	}
	defer rows.Close()

	data := make([]map[string]string, 0, 100000)

	var decoder transform.Transformer
	if exporter.config.Decoder != "" {
		enc, normalized := resolveDecoder(exporter.config.Decoder)
		if enc == nil {
			logMessage(os.Stderr, "Предупреждение", fmt.Sprintf("decoder %q не поддерживается, используется windows-1251", exporter.config.Decoder))
			enc = charmap.Windows1251
		} else if normalized != exporter.config.Decoder {
			exporter.config.Decoder = normalized
		}
		decoder = enc.NewDecoder()
	}

	for rows.Next() {
		d, err := exporter.writeRow(rows)
		if err != nil {
			return nil, 0, err
		}
		if decoder != nil {
			for k, v := range d {
				if v == "" {
					continue
				}
				decodedValue, _, err := transform.String(decoder, v)
				if err != nil {
					return nil, 0, fmt.Errorf("failed to decode value for key %s (original value: '%s'): %w", k, v, err)
				}
				d[k] = decodedValue
			}
		}
		data = append(data, d)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("failed to iterate rows: %w", err)
	}

	if len(data) == 0 {
		// Return a specific error if no data is found
		return nil, 0, apperrors.New(apperrors.DbNoData, "")
	}

	return &data, time.Since(beg).Truncate(time.Second), nil
}

func prepareQuery(query string, start time.Time, end time.Time) (string, []interface{}) {
	placeholders := []queryPlaceholder{
		{token: "{start}", value: start.Format("2006-01-02 15:04:05")},
		{token: "{end}", value: end.Format("2006-01-02 15:04:05")},
		{token: "{tag}", value: "%%"},
	}

	args := make([]interface{}, 0, len(placeholders))
	for _, placeholder := range placeholders {
		quotedToken := "'" + placeholder.token + "'"
		if strings.Contains(query, quotedToken) {
			query = strings.Replace(query, quotedToken, "?", 1)
			args = append(args, placeholder.value)
			continue
		}
		if strings.Contains(query, placeholder.token) {
			query = strings.Replace(query, placeholder.token, "?", 1)
			args = append(args, placeholder.value)
		}
	}

	return query, args
}

// сохраняет данные в файл
func (exporter *Exporter) saveData(start time.Time, end time.Time, data *[]map[string]string) (string, time.Duration, error) {
	beg := time.Now()
	fileName := exporter.generateFileName(start, end)
	outputPath := filepath.Dir(fileName)
	// проверяем путь к выходному файлу и создаем его, если не существует
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		err := os.MkdirAll(outputPath, 0755)
		if err != nil {
			return "", 0, fmt.Errorf("failed to create output path: %w", err)
		}
	}

	file, err := os.Create(fileName)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	compressor, err := compressor.NewCompressor(exporter.config.Compression, file)
	if err != nil {
		return "", 0, err
	}
	defer compressor.Close()

	encoder, err := format.NewEncoder(exporter.config.Output_format, compressor)
	if err != nil {
		return "", 0, err
	}
	encoder.SetFormatParams(exporter.getFormatParams())
	if err = encoder.Encode(*data); err != nil {
		return "", 0, err
	}

	err = exporter.saveLastPeriodDate(end)
	if err != nil {
		return "", 0, err
	}

	return fileName, time.Since(beg).Truncate(time.Second), nil
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

func formatPeriodSummary(summary periodSummary) string {
	return fmt.Sprintf(
		"[%s] Период %s -> %s | строк: %s | БД: %s | файл: %s | всего: %s | %s",
		summary.finishedAt.Format("02.01.2006 15:04:05"),
		summary.start.Format("2006-01-02 15:04:05"),
		summary.end.Format("2006-01-02 15:04:05"),
		formatCount(summary.rows),
		summary.loadDuration,
		summary.saveDuration,
		summary.totalDuration,
		summary.fileName,
	)
}

func formatCount(n int) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}

	firstGroupLen := len(s) % 3
	if firstGroupLen == 0 {
		firstGroupLen = 3
	}

	var parts []string
	parts = append(parts, s[:firstGroupLen])
	for i := firstGroupLen; i < len(s); i += 3 {
		parts = append(parts, s[i:i+3])
	}

	return strings.Join(parts, " ")
}

func resolveDecoder(name string) (*charmap.Charmap, string) {
	switch strings.ToLower(strings.ReplaceAll(name, "-", "")) {
	case "windows1251", "cp1251", "win1251":
		return charmap.Windows1251, "windows-1251"
	case "koi8r":
		return charmap.KOI8R, "koi8-r"
	default:
		return nil, ""
	}
}

func logMessage(output *os.File, title string, message string) {
	fmt.Fprintf(output, "[%s] %s | %s\n", time.Now().Format("02.01.2006 15:04:05"), title, message)
}
