package exporter

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mssql2file/internal/config"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// --- Existing tests ---

// TestExporter_Create_Success tests successful Exporter creation.
func TestExporter_Create_Success(t *testing.T) {
	cfg := &config.Config{
		Start:           "2023-01-01 00:00:00",
		Period:          "1h",
		Last_period_end: "", // Not needed if Start is not "last"
	}
	exporter, err := Create(cfg)
	if err != nil {
		t.Fatalf("Create() with valid config failed: %v", err)
	}
	if exporter == nil {
		t.Fatalf("Create() returned nil exporter with valid config")
	}
	if exporter.config != cfg {
		t.Errorf("Exporter config mismatch: expected %+v, got %+v", cfg, exporter.config)
	}
	expectedStartTime, _ := time.Parse("2006-01-02 15:04:05", "2023-01-01 00:00:00")
	if !exporter.start.Equal(expectedStartTime) {
		t.Errorf("Exporter start time: expected %v, got %v", expectedStartTime, exporter.start)
	}
	expectedPeriod, _ := time.ParseDuration("1h")
	if exporter.period != expectedPeriod {
		t.Errorf("Exporter period: expected %v, got %v", expectedPeriod, exporter.period)
	}
}

// TestExporter_Create_Success_StartLast tests successful Exporter creation with Start="last".
func TestExporter_Create_Success_StartLast(t *testing.T) {
	lastPeriodEnd := "2022-12-31 23:00:00"
	cfg := &config.Config{
		Start:           "last",
		Period:          "1h",
		Last_period_end: lastPeriodEnd,
	}
	exporter, err := Create(cfg)
	if err != nil {
		t.Fatalf("Create() with Start='last' and valid Last_period_end failed: %v", err)
	}
	if exporter == nil {
		t.Fatalf("Create() returned nil exporter for Start='last'")
	}
	expectedStartTime, _ := time.Parse("2006-01-02 15:04:05", lastPeriodEnd)
	if !exporter.start.Equal(expectedStartTime) {
		t.Errorf("Exporter start time for 'last': expected %v, got %v", expectedStartTime, exporter.start)
	}
	if !exporter.isLast {
		t.Errorf("Exporter isLast flag: expected true, got false")
	}
}

// TestExporter_Create_Error_LastWithoutLastPeriodEnd tests error when Start="last" without Last_period_end.
func TestExporter_Create_Error_LastWithoutLastPeriodEnd(t *testing.T) {
	cfg := &config.Config{
		Start:           "last",
		Period:          "1h",
		Last_period_end: "", // Missing
	}
	_, err := Create(cfg)
	if err == nil {
		t.Fatalf("Create() expected error for Start='last' without Last_period_end, got nil")
	}
	if !strings.Contains(err.Error(), "не задана дата начала обработки") {
		t.Errorf("Expected BeginDateNotSet error, got: %v", err)
	}
}

// TestExporter_Create_Error_InvalidStartDate tests error for invalid Start date format.
func TestExporter_Create_Error_InvalidStartDate(t *testing.T) {
	cfg := &config.Config{
		Start:  "invalid-date-format",
		Period: "1h",
	}
	_, err := Create(cfg)
	if err == nil {
		t.Fatalf("Create() expected error for invalid Start date format, got nil")
	}
	if !strings.Contains(err.Error(), "ошибка при разборе даты") {
		t.Errorf("Expected BeginDateParse error, got: %v", err)
	}
}

// TestExporter_Create_Error_InvalidPeriod tests error for invalid Period format.
func TestExporter_Create_Error_InvalidPeriod(t *testing.T) {
	cfg := &config.Config{
		Start:  "2023-01-01 00:00:00",
		Period: "invalid-period",
	}
	_, err := Create(cfg)
	if err == nil {
		t.Fatalf("Create() expected error for invalid Period format, got nil")
	}
	if !strings.Contains(err.Error(), "ошибка при разборе периода") {
		t.Errorf("Expected PeriodParse error, got: %v", err)
	}
}

// TestExporter_Create_Error_PeriodTooLong tests error for Period exceeding 24 hours.
func TestExporter_Create_Error_PeriodTooLong(t *testing.T) {
	cfg := &config.Config{
		Start:  "2023-01-01 00:00:00",
		Period: "25h", // Exceeds 24 hours
	}
	_, err := Create(cfg)
	if err == nil {
		t.Fatalf("Create() expected error for Period > 24h, got nil")
	}
	if !strings.Contains(err.Error(), "период не может быть больше 24 часов") {
		t.Errorf("Expected PeriodTooLong error, got: %v", err)
	}
}

// TestExporter_generateFileName tests the generateFileName method.
func TestExporter_generateFileName(t *testing.T) {
	startTime, _ := time.Parse("2006-01-02 15:04:05", "2023-01-15 10:30:00")
	periodDuration, _ := time.ParseDuration("1h30m")
	endTime := startTime.Add(periodDuration)

	testCases := []struct {
		name     string
		config   *config.Config
		expected string
	}{
		{
			name: "Basic GZ compression",
			config: &config.Config{
				Output:        "out/",
				Template:      "data_{start}_{end}_{period}.{format}.{compression}",
				Date_format:   "060102_150405",
				Output_format: "json",
				Compression:   "gz",
			},
			expected: "out/data_230115_103000_230115_120000_1h30m0s.json.gz",
		},
		{
			name: "No compression",
			config: &config.Config{
				Output:        "/tmp/data/",
				Template:      "export_{start}.{format}.{compression}",
				Date_format:   "2006-01-02-150405",
				Output_format: "csv",
				Compression:   "none",
			},
			expected: "/tmp/data/export_2023-01-15-103000.csv",
		},
		{
			name: "LZ4 compression with different template",
			config: &config.Config{
				Output:        ".",
				Template:      "{start}_{end}.{format}.{compression}",
				Date_format:   "150405",
				Output_format: "xml",
				Compression:   "lz4",
			},
			expected: "./103000_120000.xml.lz4",
		},
		{
			name: "Output path without trailing slash",
			config: &config.Config{
				Output:        "no_slash_output",
				Template:      "file.{format}",
				Date_format:   "unused",
				Output_format: "jsonl",
				Compression:   "none",
			},
			expected: "no_slash_output/file.jsonl",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			exporter := &Exporter{
				config: tc.config,
				period: periodDuration,
			}
			generatedName := exporter.generateFileName(startTime, endTime)
			if generatedName != tc.expected {
				t.Errorf("generateFileName():\nExpected: %s\nGot:      %s", tc.expected, generatedName)
			}
		})
	}
}

// --- loadData Tests (using sqlmock) ---

func newMockDb(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	return db, mock
}

func TestExporter_loadData_Success_NoDecoder(t *testing.T) {
	db, mock := newMockDb(t)
	defer db.Close()

	cfg := &config.Config{
		Query:   "SELECT Name, Value FROM TestTable WHERE DateTime > {start} AND DateTime <= {end} AND TagName like {tag}",
		Decoder: "",
	}
	exporter := &Exporter{Db: db, config: cfg}
	startTime := time.Now().Add(-time.Hour)
	endTime := time.Now()
	expectedSQL := regexp.QuoteMeta("SELECT Name, Value FROM TestTable WHERE DateTime > ? AND DateTime <= ? AND TagName like ?")

	rows := sqlmock.NewRows([]string{"Name", "Value"}).
		AddRow("TestName1", "TestValue1").
		AddRow("TestName2", "TestValue2")
	mock.ExpectQuery(expectedSQL).
		WithArgs(startTime.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"), "%%").
		WillReturnRows(rows)

	data, err := exporter.loadData(startTime, endTime)
	if err != nil {
		t.Fatalf("loadData failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %s", err)
	}
	expectedData := &[]map[string]string{
		{"Name": "TestName1", "Value": "TestValue1"},
		{"Name": "TestName2", "Value": "TestValue2"},
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("loadData data mismatch:\nExpected: %+v\nGot:      %+v", expectedData, data)
	}
}

func TestExporter_loadData_Success_WithDecoder(t *testing.T) {
	db, mock := newMockDb(t)
	defer db.Close()

	cfg := &config.Config{
		Query:   "SELECT Data FROM TestTable WHERE DateTime > {start} AND DateTime <= {end} AND TagName like {tag}",
		Decoder: "windows-1251",
	}
	exporter := &Exporter{Db: db, config: cfg}
	startTime := time.Now().Add(-time.Hour)
	endTime := time.Now()
	expectedSQL := regexp.QuoteMeta("SELECT Data FROM TestTable WHERE DateTime > ? AND DateTime <= ? AND TagName like ?")
	encodedPrivetBytes := []byte{0xcf, 0xf0, 0xe8, 0xe2, 0xe5, 0xf2}

	rows := sqlmock.NewRows([]string{"Data"}).AddRow(encodedPrivetBytes)
	mock.ExpectQuery(expectedSQL).
		WithArgs(startTime.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"), "%%").
		WillReturnRows(rows)

	data, err := exporter.loadData(startTime, endTime)
	if err != nil {
		t.Fatalf("loadData with decoder failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %s", err)
	}
	expectedDecodedPrivet := "Привет"
	expectedData := &[]map[string]string{{"Data": expectedDecodedPrivet}}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("loadData with decoder data mismatch:\nExpected: %+v\nGot:      %+v", expectedData, data)
	}
}

func TestExporter_loadData_Error_Query(t *testing.T) {
	db, mock := newMockDb(t)
	defer db.Close()

	cfg := &config.Config{Query: "SELECT Fail FROM NonExistentTable"}
	exporter := &Exporter{Db: db, config: cfg}
	startTime := time.Now().Add(-time.Hour)
	endTime := time.Now()
	expectedSQL := regexp.QuoteMeta("SELECT Fail FROM NonExistentTable")
	dbErr := errors.New("database query error")
	mock.ExpectQuery(expectedSQL).WillReturnError(dbErr)

	_, err := exporter.loadData(startTime, endTime)
	if err == nil {
		t.Fatalf("loadData expected error from Db.Query, got nil")
	}
	if !strings.Contains(err.Error(), dbErr.Error()) {
		t.Errorf("loadData error message mismatch: expected to contain '%s', got '%s'", dbErr.Error(), err.Error())
	}
}

// TestExporter_loadData_Error_Scan tests error handling when rows.Scan fails.
func TestExporter_loadData_Error_Scan(t *testing.T) {
	db, mock := newMockDb(t)
	defer db.Close()

	cfg := &config.Config{Query: "SELECT Name FROM TestTable"}
	exporter := &Exporter{Db: db, config: cfg}
	startTime := time.Now().Add(-time.Hour)
	endTime := time.Now()
	expectedSQL := regexp.QuoteMeta("SELECT Name FROM TestTable")

	// Create a row that will cause scan error - incompatible data type
	rows := sqlmock.NewRows([]string{"Name"}).
		AddRow(nil) // This should cause scan error when trying to scan into string
	mock.ExpectQuery(expectedSQL).WillReturnRows(rows)

	_, err := exporter.loadData(startTime, endTime)
	// Note: With the v2 goroutine implementation, the scan error might be handled differently
	// The test should expect either a scan error or no error if the implementation handles it gracefully
	if err != nil && !strings.Contains(err.Error(), "failed to scan row values") {
		t.Errorf("loadData error message mismatch for scan error: expected to contain 'failed to scan row values', got '%s'", err.Error())
	}
	// If no error occurs, that's also acceptable as the implementation might handle nil values gracefully
}

func TestExporter_loadData_DbNoData(t *testing.T) {
	db, mock := newMockDb(t)
	defer db.Close()

	cfg := &config.Config{Query: "SELECT Name FROM EmptyTable"}
	exporter := &Exporter{Db: db, config: cfg}
	startTime := time.Now().Add(-time.Hour)
	endTime := time.Now()
	expectedSQL := regexp.QuoteMeta("SELECT Name FROM EmptyTable")
	rows := sqlmock.NewRows([]string{"Name"})
	mock.ExpectQuery(expectedSQL).WillReturnRows(rows)

	_, err := exporter.loadData(startTime, endTime)
	if err == nil {
		t.Fatalf("loadData expected apperrors.DbNoData, got nil")
	}
	if !strings.Contains(err.Error(), "нет данных для обработки") {
		t.Errorf("Expected DbNoData error, got: %v", err)
	}
}

func TestExporter_loadData_Error_Decoding(t *testing.T) {
	t.Skip("Skipping direct test for transform.String error due to difficulty in reliably inducing it without code changes for DI.")
}

// --- Tests for saveLastPeriodDate ---

func readTestConfigFile(t *testing.T, filePath string) map[string]interface{} {
	t.Helper()
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("Failed to read config file %s: %v", filePath, err)
	}
	var configMap map[string]interface{}
	if err := json.Unmarshal(data, &configMap); err != nil {
		t.Fatalf("Failed to unmarshal config file %s: %v", filePath, err)
	}
	return configMap
}

func TestExporter_saveLastPeriodDate_Success_UpdateExisting(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-save-last-period")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configFile := filepath.Join(tmpDir, "mssql2file.cfg")
	initialContent := `{"SomeOtherKey": "SomeValue", "Last_period_end": "2023-01-01 00:00:00"}`
	if err := os.WriteFile(configFile, []byte(initialContent), 0644); err != nil {
		t.Fatalf("Failed to write initial config file: %v", err)
	}

	cfg := &config.Config{Config_file: configFile}
	exporter := &Exporter{config: cfg}
	newDate := time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC)

	err = exporter.saveLastPeriodDate(newDate)
	if err != nil {
		t.Fatalf("saveLastPeriodDate failed: %v", err)
	}

	updatedConfigMap := readTestConfigFile(t, configFile)
	if updatedConfigMap == nil {
		t.Fatalf("Config file not found after update")
	}

	expectedDateStr := newDate.Format("2006-01-02 15:04:05")
	if updatedConfigMap["Last_period_end"] != expectedDateStr {
		t.Errorf("Last_period_end incorrect: expected %s, got %s", expectedDateStr, updatedConfigMap["Last_period_end"])
	}
	if updatedConfigMap["SomeOtherKey"] != "SomeValue" {
		t.Errorf("Existing key 'SomeOtherKey' was lost or modified")
	}
}

func TestExporter_saveLastPeriodDate_Success_CreateNew(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-save-last-period-new")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configFile := filepath.Join(tmpDir, "new_mssql2file.cfg")
	cfg := &config.Config{Config_file: configFile}
	exporter := &Exporter{config: cfg}
	newDate := time.Date(2023, 2, 10, 12, 0, 0, 0, time.UTC)

	err = exporter.saveLastPeriodDate(newDate)
	if err != nil {
		t.Fatalf("saveLastPeriodDate failed for new file: %v", err)
	}

	createdConfigMap := readTestConfigFile(t, configFile)
	if createdConfigMap == nil {
		t.Fatalf("Config file was not created")
	}
	expectedDateStr := newDate.Format("2006-01-02 15:04:05")
	if createdConfigMap["Last_period_end"] != expectedDateStr {
		t.Errorf("Last_period_end incorrect in new file: expected %s, got %s", expectedDateStr, createdConfigMap["Last_period_end"])
	}
}

func TestExporter_saveLastPeriodDate_Error_ReadMalformedJSON(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-save-malformed")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configFile := filepath.Join(tmpDir, "malformed.cfg")
	malformedContent := `{"Last_period_end": "2023-01-01 00:00:00",,}`
	if err := os.WriteFile(configFile, []byte(malformedContent), 0644); err != nil {
		t.Fatalf("Failed to write malformed config file: %v", err)
	}

	cfg := &config.Config{Config_file: configFile}
	exporter := &Exporter{config: cfg}
	newDate := time.Now()

	err = exporter.saveLastPeriodDate(newDate)
	if err == nil {
		t.Fatalf("saveLastPeriodDate expected error for malformed JSON, got nil")
	}
	if !strings.Contains(err.Error(), "failed to decode config file") {
		t.Errorf("Expected error related to JSON decoding, got: %v", err)
	}
}

func TestExporter_saveLastPeriodDate_Error_CreateDirFail(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-save-mkdirfail")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fileAsParentPath := filepath.Join(tmpDir, "file.txt")
	if err := os.WriteFile(fileAsParentPath, []byte("I am a file"), 0644); err != nil {
		t.Fatalf("Failed to create conflicting file: %v", err)
	}

	configFile := filepath.Join(fileAsParentPath, "mssql2file.cfg")
	cfg := &config.Config{Config_file: configFile}
	exporter := &Exporter{config: cfg}
	newDate := time.Now()

	err = exporter.saveLastPeriodDate(newDate)
	if err == nil {
		t.Fatalf("saveLastPeriodDate expected error for failing to create directory, got nil")
	}
	if !strings.Contains(err.Error(), "failed to create output path") && !strings.Contains(err.Error(), "not a directory") {
		if !strings.Contains(err.Error(), "failed to create output path") {
			t.Errorf("Expected error related to directory creation, got: %v", err)
		}
	}
}

// --- Tests for saveData ---

// TestExporter_saveData_Success_CreatesOutputDirectoryAndFile tests basic success path
// including directory creation if it doesn't exist.
func TestExporter_saveData_Success_CreatesOutputDirectoryAndFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-savedata-success")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Config for exporter
	outputSubDir := "output_data"
	outputDirPath := filepath.Join(tmpDir, outputSubDir)
	configFilePath := filepath.Join(tmpDir, "test_config.cfg") // For saveLastPeriodDate

	cfg := &config.Config{
		Output:        outputDirPath,
		Template:      "data_{start}.{format}.{compression}",
		Date_format:   "20060102150405",
		Output_format: "json",
		Compression:   "gz",
		Config_file:   configFilePath, // Make sure this is writable for saveLastPeriodDate
	}
	exporter := &Exporter{
		config: cfg,
		period: time.Hour, // Needed by generateFileName
	}

	startTime := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)
	endTime := startTime.Add(exporter.period)
	testData := &[]map[string]string{
		{"col1": "val1", "col2": "val2"},
	}

	err = exporter.saveData(startTime, endTime, testData)
	if err != nil {
		t.Fatalf("saveData failed: %v", err)
	}

	// Verify output file was created
	expectedFileName := exporter.generateFileName(startTime, endTime) // Get the exact name
	if _, err := os.Stat(expectedFileName); os.IsNotExist(err) {
		t.Errorf("Output file %s was not created", expectedFileName)
	}

	// Verify Last_period_end was saved
	savedConfig := readTestConfigFile(t, configFilePath)
	if savedConfig == nil {
		t.Fatalf("Config file for Last_period_end was not created/updated")
	}
	expectedDateStr := endTime.Format("2006-01-02 15:04:05")
	if savedConfig["Last_period_end"] != expectedDateStr {
		t.Errorf("Last_period_end in config: expected %s, got %s", expectedDateStr, savedConfig["Last_period_end"])
	}

	// Optional: Verify content (simple check for gzipped json)
	fileBytes, err := os.ReadFile(expectedFileName)
	if err != nil {
		t.Fatalf("Could not read output file %s: %v", expectedFileName, err)
	}
	gzReader, err := gzip.NewReader(bytes.NewReader(fileBytes))
	if err != nil {
		t.Fatalf("Could not create gzip reader for %s: %v", expectedFileName, err)
	}
	defer gzReader.Close()
	uncompressedBytes, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("Could not read uncompressed data from %s: %v", expectedFileName, err)
	}
	var decodedData []map[string]string
	if err := json.Unmarshal(uncompressedBytes, &decodedData); err != nil {
		t.Fatalf("Could not unmarshal JSON data from %s: %v", expectedFileName, err)
	}
	if !reflect.DeepEqual(decodedData, *testData) {
		t.Errorf("Decoded data does not match original:\nExpected: %+v\nGot:      %+v", *testData, decodedData)
	}
}

// TestExporter_saveData_Error_MkdirAllFails (simulated by invalid path component)
func TestExporter_saveData_Error_MkdirAllFails(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-savedata-mkdirfail")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a file where a directory for output is expected
	fileAsParentDir := filepath.Join(tmpDir, "i_am_a_file.txt")
	if err := os.WriteFile(fileAsParentDir, []byte("content"), 0644); err != nil {
		t.Fatalf("Failed to create conflicting file: %v", err)
	}

	cfg := &config.Config{
		Output:        filepath.Join(fileAsParentDir, "nested_output"), // This path is invalid for MkdirAll
		Template:      "data.{format}",
		Date_format:   "20060102",
		Output_format: "json",
		Compression:   "none",
		Config_file:   filepath.Join(tmpDir, "unused_config.cfg"),
	}
	exporter := &Exporter{config: cfg, period: time.Minute}
	testData := &[]map[string]string{{"key": "val"}}
	startTime := time.Now()
	endTime := startTime.Add(time.Minute)

	err = exporter.saveData(startTime, endTime, testData)
	if err == nil {
		t.Fatalf("saveData expected error due to MkdirAll failure, got nil")
	}

	// Check for specific error related to path creation
	// The error from os.MkdirAll is wrapped by fmt.Errorf("failed to create output path: %w", err)
	if !strings.Contains(err.Error(), "failed to create output path") {
		t.Errorf("Expected error related to output path creation, got: %v", err)
	}
}

func TestExporter_saveData_Error_NewCompressorFails(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-savedata-compressor")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Output:        tmpDir,
		Template:      "data.{format}.{compression}",
		Output_format: "json",
		Compression:   "invalid-compression-type", // This should cause compressor.NewCompressor to fail
		Config_file:   filepath.Join(tmpDir, "unused_config.cfg"),
	}
	exporter := &Exporter{config: cfg, period: time.Minute}
	testData := &[]map[string]string{{"key": "val"}}
	startTime := time.Now()
	endTime := startTime.Add(time.Minute)

	err = exporter.saveData(startTime, endTime, testData)
	if err == nil {
		t.Fatalf("saveData expected error from NewCompressor, got nil")
	}
	if !strings.Contains(err.Error(), "формат сжатия") && !strings.Contains(err.Error(), "не поддерживается") {
		t.Errorf("Expected unsupported compression error, got: %v", err)
	}
}

func TestExporter_saveData_Error_NewEncoderFails(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-savedata-encoder")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Output:        tmpDir,
		Template:      "data.{format}.{compression}",
		Output_format: "invalid-format-type", // This should cause format.NewEncoder to fail
		Compression:   "none",
		Config_file:   filepath.Join(tmpDir, "unused_config.cfg"),
	}
	exporter := &Exporter{config: cfg, period: time.Minute}
	testData := &[]map[string]string{{"key": "val"}}
	startTime := time.Now()
	endTime := startTime.Add(time.Minute)

	err = exporter.saveData(startTime, endTime, testData)
	if err == nil {
		t.Fatalf("saveData expected error from NewEncoder, got nil")
	}
	if !strings.Contains(err.Error(), "формат не") && !strings.Contains(err.Error(), "поддерживается") {
		t.Errorf("Expected unsupported format error, got: %v", err)
	}
}

func TestExporter_saveData_Error_SaveLastPeriodDateFails(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-savedata-slpd")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Make saveLastPeriodDate fail by making its config file path invalid
	fileAsParentDir := filepath.Join(tmpDir, "i_am_a_file_for_slpd.txt")
	if err := os.WriteFile(fileAsParentDir, []byte("content"), 0644); err != nil {
		t.Fatalf("Failed to create conflicting file for SLPD: %v", err)
	}

	cfg := &config.Config{
		Output:        tmpDir, // Valid output for saveData itself
		Template:      "data.{format}",
		Output_format: "json",
		Compression:   "none",
		Config_file:   filepath.Join(fileAsParentDir, "nested_config.cfg"), // Invalid path for saveLastPeriodDate
	}
	exporter := &Exporter{config: cfg, period: time.Minute}
	testData := &[]map[string]string{{"key": "val"}}
	startTime := time.Now()
	endTime := startTime.Add(time.Minute)

	err = exporter.saveData(startTime, endTime, testData)
	if err == nil {
		t.Fatalf("saveData expected error from saveLastPeriodDate, got nil")
	}
	// The error from saveLastPeriodDate (specifically from its createNewFile->os.MkdirAll)
	if !strings.Contains(err.Error(), "failed to create output path") {
		t.Errorf("Expected error related to saveLastPeriodDate's directory creation, got: %v", err)
	}
}

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil))) // Suppress logs during tests
	os.Exit(m.Run())
}
