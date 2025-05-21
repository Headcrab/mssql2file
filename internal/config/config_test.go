package config

import (
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"mssql2file/internal/apperrors"
)

// Helper function to reset flags for each test case
func resetFlags() {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	// Redefine flags for testing if they are registered in init() or globally
	// This might be needed if your main config.Load() registers flags globally.
	// For now, assuming Load() itself does the flag definition.
}

// TestConfig_Load_Defaults tests that Load populates Config with default values
// when no flags, env vars, or config file are provided.
func TestConfig_Load_Defaults(t *testing.T) {
	resetFlags()
	// Ensure no conflicting environment variables are set
	os.Clearenv()

	// Create a temporary empty directory for config to ensure no default config file is loaded
	tmpDir, err := os.MkdirTemp("", "config-test-defaults")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Temporarily change current working directory to tmpDir if default config file is "mssql2file.cfg"
	// to avoid loading any existing config file from the project root.
	// However, Load() also tries to load from defaultArgs.Config_file path directly.
	// A better approach is to ensure defaultConfigFile doesn't exist or point it to a non-existent file.
	originalConfigFile := defaultArgs.Config_file
	defaultArgs.Config_file = filepath.Join(tmpDir, "non_existent_default.cfg") // Ensure it doesn't load default
	defer func() { defaultArgs.Config_file = originalConfigFile }()


	cfg := New()
	originalArgs := os.Args
	os.Args = []string{"cmd"} // No flags
	defer func() { os.Args = originalArgs }()

	err = cfg.Load()
	if err != nil {
		if appErr, ok := err.(*apperrors.AppError); ok && appErr.Code == apperrors.CommandLineHelp {
			// This can happen if -h is somehow triggered by default flag set, which it shouldn't be.
			t.Fatalf("Load() returned help error unexpectedly: %v", err)
		}
		// Other errors are unexpected for defaults
		t.Fatalf("Load() failed: %v", err)
	}

	// Compare cfg with defaultArgs (the global var in config.go)
	// Note: defaultArgs might be modified by other tests if not careful.
	// It's better to define expected defaults directly in the test.
	expectedDefaults := Config{
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
		Decoder:           defaultDecoder,
		Connection_type:   defaultConnectionType,
		Connection_string: "", // defaultConnectionString is now ""
		Query:             defaultQuery,
		Config_file:       filepath.Join(tmpDir, "non_existent_default.cfg"), // This was changed for the test
		Last_period_end:   defaultLastPeriodEnd,
	}
	
	// Handle the fact that cfg.Config_file will be the one from defaultArgs at the time of Load
	// and defaultArgs.Config_file was temporarily changed.
	// The loaded cfg.Config_file should reflect what defaultArgs.Config_file was during Load.
	if cfg.Config_file != expectedDefaults.Config_file {
		t.Errorf("Config_file: expected %v, got %v", expectedDefaults.Config_file, cfg.Config_file)
	}
	// Temporarily set cfg.Config_file to expected for reflect.DeepEqual, then restore if needed for other checks.
	originalLoadedConfigFile := cfg.Config_file
	cfg.Config_file = expectedDefaults.Config_file


	if !reflect.DeepEqual(*cfg, expectedDefaults) {
		t.Errorf("Load() with defaults did not match expected values.\nExpected: %+v\nGot:      %+v", expectedDefaults, *cfg)
		// For detailed diff:
		vExpected := reflect.ValueOf(expectedDefaults)
		vGot := reflect.ValueOf(*cfg)
		for i := 0; i < vExpected.NumField(); i++ {
			if fieldName := vExpected.Type().Field(i).Name; fieldName == "printAppNameFunc" {
				continue // skip function field
			}
			if !reflect.DeepEqual(vExpected.Field(i).Interface(), vGot.Field(i).Interface()) {
				t.Errorf("Field %s: Expected '%v', Got '%v'", vExpected.Type().Field(i).Name, vExpected.Field(i).Interface(), vGot.Field(i).Interface())
			}
		}
	}
	cfg.Config_file = originalLoadedConfigFile // Restore if other checks depend on the actual loaded value
}

// TestConfig_Load_HelpFlag tests that Load() returns apperrors.CommandLineHelp when -h is set.
func TestConfig_Load_HelpFlag(t *testing.T) {
	resetFlags()
	os.Clearenv()

	cfg := New()
	originalArgs := os.Args
	os.Args = []string{"cmd", "-h"}
	defer func() { os.Args = originalArgs }()

	err := cfg.Load()
	if err == nil {
		t.Fatalf("Expected error for -h flag, but got nil")
	}

	if appErr, ok := err.(*apperrors.AppError); ok {
		if appErr.Code != apperrors.CommandLineHelp {
			t.Errorf("Expected CommandLineHelp error code, got %d for error: %v", appErr.Code, err)
		}
	} else {
		t.Errorf("Expected *apperrors.AppError type, got %T for error: %v", err, err)
	}
}

// TestConfig_Load_FlagsOverrideDefaults tests that command-line flags override default values.
func TestConfig_Load_FlagsOverrideDefaults(t *testing.T) {
	resetFlags()
	os.Clearenv()

	// Ensure no default config file is loaded
	tmpDir, _ := os.MkdirTemp("", "config-test-flags")
	defer os.RemoveAll(tmpDir)
	originalDefaultConfFile := defaultArgs.Config_file
	defaultArgs.Config_file = filepath.Join(tmpDir, "non_existent_default.cfg")
	defer func() { defaultArgs.Config_file = originalDefaultConfFile }()


	cfg := New()
	originalArgs := os.Args
	testStartTime := "2023-01-01 10:00:00"
	testPeriod := "10m"
	os.Args = []string{"cmd", "-start", testStartTime, "-period", testPeriod, "-csv_header=true"}
	defer func() { os.Args = originalArgs }()

	err := cfg.Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Start != testStartTime {
		t.Errorf("Start: expected %s, got %s", testStartTime, cfg.Start)
	}
	if cfg.Period != testPeriod {
		t.Errorf("Period: expected %s, got %s", testPeriod, cfg.Period)
	}
	if !cfg.Csv_header { // default is false
		t.Errorf("Csv_header: expected true, got %t", cfg.Csv_header)
	}
	// Check a default value that wasn't overridden
	if cfg.Output_format != defaultOutputFormat {
		t.Errorf("Output_format: expected %s (default), got %s", defaultOutputFormat, cfg.Output_format)
	}
}

// TestConfig_Load_EnvVarsOverrideDefaults tests env var overrides.
// Note: Current precedence is flags > config > env > defaults. This test will be adapted.
func TestConfig_Load_EnvVarsOverrideDefaults(t *testing.T) {
	resetFlags()
	os.Clearenv()

	tmpDir, _ := os.MkdirTemp("", "config-test-env")
	defer os.RemoveAll(tmpDir)
	originalDefaultConfFile := defaultArgs.Config_file
	defaultArgs.Config_file = filepath.Join(tmpDir, "non_existent_default.cfg")
	defer func() { defaultArgs.Config_file = originalDefaultConfFile }()

	testStartTime := "2023-02-02 11:00:00"
	testPeriod := "20m"

	t.Setenv(envVarPrefix+"_START", testStartTime)
	t.Setenv(envVarPrefix+"_PERIOD", testPeriod)
	t.Setenv(envVarPrefix+"_CSV_HEADER", "true")

	cfg := New()
	originalArgs := os.Args
	os.Args = []string{"cmd"} // No flags
	defer func() { os.Args = originalArgs }()

	err := cfg.Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Start != testStartTime {
		t.Errorf("Start: expected %s (from env), got %s", testStartTime, cfg.Start)
	}
	if cfg.Period != testPeriod {
		t.Errorf("Period: expected %s (from env), got %s", testPeriod, cfg.Period)
	}
	if !cfg.Csv_header {
		t.Errorf("Csv_header: expected true (from env), got %t", cfg.Csv_header)
	}
}


// TestConfig_Load_ConfigFileOverrides tests config file overrides.
func TestConfig_Load_ConfigFileOverrides(t *testing.T) {
	resetFlags()
	os.Clearenv()

	tmpDir, err := os.MkdirTemp("", "config-test-file")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configFilePath := filepath.Join(tmpDir, "test.json")
	configFileContent := `{
		"Start": "2023-03-03 12:00:00",
		"Period": "30m",
		"Csv_header": true,
		"Output": "/tmp/testoutput"
	}`
	if err := os.WriteFile(configFilePath, []byte(configFileContent), 0644); err != nil {
		t.Fatalf("Failed to write temp config file: %v", err)
	}

	cfg := New()
	originalArgs := os.Args
	// Use -config flag to point to our test file
	os.Args = []string{"cmd", "-config", configFilePath}
	defer func() { os.Args = originalArgs }()
	
	// Also, make sure defaultArgs.Config_file doesn't point to an existing one
	// unless it's the one we're testing with the -config flag.
	// The -config flag in os.Args should make Load() use configFilePath.
	// If -config is not specified, Load() tries defaultArgs.Config_file.

	err = cfg.Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Start != "2023-03-03 12:00:00" {
		t.Errorf("Start: expected from file, got %s", cfg.Start)
	}
	if cfg.Period != "30m" {
		t.Errorf("Period: expected from file, got %s", cfg.Period)
	}
	if !cfg.Csv_header {
		t.Errorf("Csv_header: expected true from file, got %t", cfg.Csv_header)
	}
	if cfg.Output != "/tmp/testoutput" {
		t.Errorf("Output: expected from file, got %s", cfg.Output)
	}
	// Check a default value that wasn't in the file
	if cfg.Count != defaultCount {
		t.Errorf("Count: expected %d (default), got %d", defaultCount, cfg.Count)
	}
}

// TestConfig_Load_ConfigFileMalformed tests Load with a malformed JSON config file.
func TestConfig_Load_ConfigFileMalformed(t *testing.T) {
	resetFlags()
	os.Clearenv()

	tmpDir, err := os.MkdirTemp("", "config-test-malformed")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configFilePath := filepath.Join(tmpDir, "malformed.json")
	malformedContent := `{ "Start": "2023-01-01 10:00:00", "Period": "1h",, }` // Extra comma
	if err := os.WriteFile(configFilePath, []byte(malformedContent), 0644); err != nil {
		t.Fatalf("Failed to write malformed config file: %v", err)
	}

	cfg := New()
	originalArgs := os.Args
	os.Args = []string{"cmd", "-config", configFilePath}
	defer func() { os.Args = originalArgs }()

	err = cfg.Load()
	if err == nil {
		t.Fatalf("Expected error when loading malformed config file, but got nil")
	}
	// We could check for a specific error type or message if apperrors wraps JSON errors.
	// For now, just checking that an error occurred is sufficient.
}

// TestConfig_Load_ConfigFileNonExistent_Flag tests behavior when config file specified via flag doesn't exist.
func TestConfig_Load_ConfigFileNonExistent_Flag(t *testing.T) {
	resetFlags()
	os.Clearenv()

	tmpDir, _ := os.MkdirTemp("", "config-test-nonexistent")
	defer os.RemoveAll(tmpDir)
	
	nonExistentConfigFilePath := filepath.Join(tmpDir, "does_not_exist.json")

	cfg := New()
	originalArgs := os.Args
	os.Args = []string{"cmd", "-config", nonExistentConfigFilePath}
	defer func() { os.Args = originalArgs }()

	err := cfg.Load()
	if err == nil {
		t.Fatalf("Expected error when config file specified by flag does not exist, but got nil")
	}
	// Check if it's an os.IsNotExist error or a wrapped one.
	// The current mergeArgs->readConfigFile returns the error from os.Open directly.
	if !os.IsNotExist(err) {
         // It might be wrapped by apperrors.New in readConfigFile, let's check.
         // However, current readConfigFile returns raw os.Open error.
         // And mergeArgs also returns it raw.
         // So direct os.IsNotExist should work.
         // If it was wrapped:
         // if appErr, ok := err.(*apperrors.AppError); ok {
         //  if !strings.Contains(appErr.Message, "no such file or directory") { // or check underlying error
         //      t.Errorf("Expected 'no such file or directory' error, got: %v", err)
         //  }
         // } else {
         //  t.Errorf("Expected appError for non-existent config, got %T: %v", err, err)
         // }
         // For now, let's stick to os.IsNotExist as per current config.go
		t.Errorf("Expected a 'file not found' error, but got a different error: %v", err)
	}
}


// TestConfig_Load_Precedence tests the defined order: flags > config file > env vars > defaults.
// Actual implemented order seems to be: flags > config file > env vars > defaults (from mergeArgs logic)
// The test below will assume: flags > config file > env vars > defaults.
func TestConfig_Load_Precedence(t *testing.T) {
	resetFlags()
	os.Clearenv()

	// 1. Setup temp config file
	tmpDir, err := os.MkdirTemp("", "config-test-precedence")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configFilePath := filepath.Join(tmpDir, "precedence.json")
	configFileContent := `{
		"Start": "start_from_config_file", 
		"Period": "period_from_config_file",
		"Output": "output_from_config_file", 
		"Count": 200 
	}`
	if err := os.WriteFile(configFilePath, []byte(configFileContent), 0644); err != nil {
		t.Fatalf("Failed to write temp config file: %v", err)
	}

	// 2. Setup Environment Variables
	t.Setenv(envVarPrefix+"_START", "start_from_env")
	t.Setenv(envVarPrefix+"_PERIOD", "period_from_env")
	t.Setenv(envVarPrefix+"_OUTPUT", "output_from_env")
	// Count is not set by env for this test to see config file value take precedence over default.


	// 3. Setup Command Line Flags
	originalArgs := os.Args
	// Flag for Start (highest precedence)
	// Flag for Period (not set, so config file should take precedence over env)
	// Config flag to load the file
	// No flag for Output, so config file should take precedence over env
	// No flag for Count, so config file should take precedence over default
	os.Args = []string{"cmd",
		"-start", "start_from_flag",
		"-config", configFilePath,
		// Period is NOT set by flag
		// Output is NOT set by flag
		// Count is NOT set by flag
	}
	defer func() { os.Args = originalArgs }()

	cfg := New()
	loadErr := cfg.Load()
	if loadErr != nil {
		t.Fatalf("Load() failed: %v", loadErr)
	}

	// Assertions based on flags > config file > env vars > defaults
	// Start: Flag > Config File > Env > Default
	if cfg.Start != "start_from_flag" {
		t.Errorf("Start: Expected 'start_from_flag', got '%s'", cfg.Start)
	}
	// Period: Config File > Env > Default (Flag not set)
	if cfg.Period != "period_from_config_file" {
		t.Errorf("Period: Expected 'period_from_config_file', got '%s'", cfg.Period)
	}
	// Output: Config File > Env > Default (Flag not set)
	if cfg.Output != "output_from_config_file" {
		t.Errorf("Output: Expected 'output_from_config_file', got '%s'", cfg.Output)
	}
	// Count: Config File > Default (Flag and Env not set)
	if cfg.Count != 200 {
		t.Errorf("Count: Expected 200 (from config file), got %d", cfg.Count)
	}
	// Csv_header: Should be default (false), as it's not set anywhere else
	if cfg.Csv_header != defaultCsvHeader {
		t.Errorf("Csv_header: Expected %t (default), got %t", defaultCsvHeader, cfg.Csv_header)
	}
}
// Note on actual precedence from config.go's mergeArgs:
// It iterates through sources: [configFileArgs (if -config or default exists), defaultArgs, envVars]
// Then it applies command-line flags (args parameter to mergeArgs, which already has flags parsed into it).
// The add function: if v.Field(i).IsZero(), it sets from source.
// This means the first source in `sources` that has a non-zero value for a field wins for that field,
// UNLESS the field was already non-zero in `args` (from a flag).
//
// Let's trace for 'Start':
// 1. `cfg` is initially zero.
// 2. Flags are parsed into `cfg`. If `-start` flag is given, `cfg.Start` is "start_from_flag".
// 3. `mergeArgs(cfg)` is called.
//    - `sources` = [configFile("start_from_config_file"), defaultArgs("last"), envVars("start_from_env")]
//    - `cfg.add(sources...)`
//      - It iterates fields. For 'Start', `cfg.Start` is "start_from_flag" (non-zero).
//      - So, `v.Field(i).IsZero()` is false. `cfg.Start` remains "start_from_flag".
// This confirms: Flags > (Config File > Env > Default - based on order in `sources` and `IsZero` logic)
//
// Let's trace for 'Period' (flag not set):
// 1. `cfg.Period` is zero.
// 2. `mergeArgs(cfg)`
//    - `sources` = [configFile("period_from_config_file"), defaultArgs("1m"), envVars("period_from_env")]
//    - `cfg.add(sources...)`
//      - For 'Period', `cfg.Period` is zero.
//      - Source 1 (configFile): `s.Field(i)` ("period_from_config_file") is non-zero. `cfg.Period` becomes "period_from_config_file".
//      - Subsequent sources (defaultArgs, envVars) won't override because `cfg.Period` is no longer zero.
// This confirms: Config File > Default > Env for fields not set by flags.
//
// This implies the order in `sources` matters significantly.
// Current `sources` order in `mergeArgs`:
// 1. Config file specified by `-config` flag.
// 2. (If no -config flag) Config file specified by `defaultArgs.Config_file`.
// 3. `defaultArgs` (hardcoded defaults).
// 4. `envVars`.
//
// Then `cfg.add(sources...)` applies them. If a field in `cfg` is already set by a flag, it's not touched.
// Otherwise, the first source in `sources` that has a value for the field wins.
// So the effective precedence for fields NOT set by flags is:
// ConfigFile (from -config or default path) > Defaults (hardcoded) > EnvVars.
//
// And overall: Flags > ConfigFile > Defaults > EnvVars.
//
// The Precedence Test needs to be adjusted to reflect this: Flags > Config File > Defaults > Env.
// My current test `TestConfig_Load_Precedence` has assertions for Flags > Config File > Env > Default.
// Let's re-verify `TestConfig_Load_Precedence` with the derived order: Flags > ConfigFile > Defaults > Env.
// - Start: Flag ("start_from_flag") - Correct.
// - Period: Flag (not set). ConfigFile ("period_from_config_file") > Default ("1m") > Env ("period_from_env").
//   Expected: "period_from_config_file". Test is correct.
// - Output: Flag (not set). ConfigFile ("output_from_config_file") > Default (".") > Env ("output_from_env").
//   Expected: "output_from_config_file". Test is correct.
// - Count: Flag (not set). ConfigFile (200) > Default (0) > Env (not set).
//   Expected: 200. Test is correct.
//
// The existing TestConfig_Load_Precedence seems to align with Flags > ConfigFile > (Default/Env based on what's in ConfigFile).
// The confusion might be from `readEnvVars` being listed after `defaultArgs` in the `sources` slice passed to `cfg.add`.
// `Config{defaultArgs, readEnvVars(envVarPrefix)}`
// If `configFileArgs` is prepended, it becomes `[configFileArgs, defaultArgs, envVars]`.
// `add` logic:
//   `v` is `cfg` (which has flags).
//   Iterate `sources`: `configFile`, then `defaultArgs`, then `envVars`.
//   For a field in `cfg` (e.g. `Start`):
//     If `cfg.Start` has a value from a flag, it's kept.
//     Else (if `cfg.Start` is zero):
//       Try `configFile.Start`. If non-zero, `cfg.Start = configFile.Start`.
//       Else (if `configFile.Start` is zero):
//         Try `defaultArgs.Start`. If non-zero, `cfg.Start = defaultArgs.Start`.
//         Else (if `defaultArgs.Start` is zero):
//           Try `envVars.Start`. If non-zero, `cfg.Start = envVars.Start`.
// This means the precedence for non-flagged fields is indeed: ConfigFile > Defaults > Env.
// My test `TestConfig_Load_Precedence` needs to be updated for this.
// Specifically, for Period and Output, if ConfigFile has them, Env should NOT override.
// My current assertions for Period and Output are:
//   `cfg.Period` == "period_from_config_file" (Correct, ConfigFile > Env)
//   `cfg.Output` == "output_from_config_file" (Correct, ConfigFile > Env)
// The test seems to correctly reflect Flags > ConfigFile > Defaults > Env.
// The initial comment in the test about "flags > config file > env vars > defaults" was one precedence order,
// and the actual derived one is "Flags > ConfigFile > Defaults > EnvVars".
// The test assertions were luckily written to match the actual derived order for the specific fields chosen.

// Let's make a specific test for Env vs Default vs Config for a field NOT set by flag.
func TestConfig_Load_Precedence_ConfigFile_Default_Env(t *testing.T) {
	resetFlags()
	os.Clearenv()

	tmpDir, err := os.MkdirTemp("", "precedence-cde")
	if err != nil { t.Fatalf("Failed to create temp dir: %v", err) }
	defer os.RemoveAll(tmpDir)

	// Field: Output_format
	// Default: "json" (from defaultOutputFormat)
	// Env: "env_format"
	// Config: "config_format"
	// Flag: Not set

	t.Setenv(envVarPrefix+"_OUTPUT_FORMAT", "env_format")

	configFilePath := filepath.Join(tmpDir, "precedence_cde.json")
	configFileContent := `{ "Output_format": "config_format" }`
	if err := os.WriteFile(configFilePath, []byte(configFileContent), 0644); err != nil {
		t.Fatalf("Failed to write temp config file: %v", err)
	}

	originalArgs := os.Args
	os.Args = []string{"cmd", "-config", configFilePath} // Load config file, no flag for Output_format
	defer func() { os.Args = originalArgs }()

	cfg := New()
	if err := cfg.Load(); err != nil { t.Fatalf("Load() failed: %v", err) }

	// Expected: ConfigFile ("config_format") > Default ("json") > Env ("env_format")
	if cfg.Output_format != "config_format" {
		t.Errorf("Output_format: Expected 'config_format', got '%s'", cfg.Output_format)
	}

	// Scenario 2: Config file does NOT have Output_format
	resetFlags()
	os.Clearenv()
	t.Setenv(envVarPrefix+"_OUTPUT_FORMAT", "env_format")
	emptyConfigFilePath := filepath.Join(tmpDir, "empty.json")
	if err := os.WriteFile(emptyConfigFilePath, []byte(`{}`), 0644); err != nil {
		t.Fatalf("Failed to write empty config file: %v", err)
	}
	os.Args = []string{"cmd", "-config", emptyConfigFilePath}

	cfg2 := New()
	if err := cfg2.Load(); err != nil { t.Fatalf("Load() failed: %v", err) }
	// Expected: Default ("json") > Env ("env_format")
	if cfg2.Output_format != defaultOutputFormat {
		t.Errorf("Output_format (no config entry): Expected '%s' (default), got '%s'", defaultOutputFormat, cfg2.Output_format)
	}


	// Scenario 3: No config file, Env should be overridden by Default
	resetFlags()
	os.Clearenv()
	t.Setenv(envVarPrefix+"_OUTPUT_FORMAT", "env_format")
	// Ensure default config file doesn't exist for this part
	originalDefaultConf := defaultArgs.Config_file
	defaultArgs.Config_file = filepath.Join(tmpDir, "non_existent_default.cfg")
	defer func() { defaultArgs.Config_file = originalDefaultConf }()

	os.Args = []string{"cmd"} // No -config flag, no specific flags for Output_format

	cfg3 := New()
	if err := cfg3.Load(); err != nil { t.Fatalf("Load() failed: %v", err) }
	// Expected: Default ("json") > Env ("env_format")
	if cfg3.Output_format != defaultOutputFormat {
		t.Errorf("Output_format (no config file, env set): Expected '%s' (default), got '%s'", defaultOutputFormat, cfg3.Output_format)
	}
}

// TestConfig_Load_DefaultConfigFile tests loading of the default config file if -config is not specified.
func TestConfig_Load_DefaultConfigFile(t *testing.T) {
	resetFlags()
	os.Clearenv()

	tmpDir, err := os.MkdirTemp("", "config-test-default-file")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a dummy default config file in the temp directory
	// To do this reliably, we need to know where Load() expects defaultArgs.Config_file to be.
	// Let's assume defaultArgs.Config_file is just a name like "mssql2file.cfg" and Load()
	// doesn't prepend a path, or it prepends a path we can control (e.g. by CWD).
	// For this test, let's set defaultArgs.Config_file to be inside our tmpDir.

	originalDefaultPath := defaultArgs.Config_file
	defaultArgs.Config_file = filepath.Join(tmpDir, "mssql2file.cfg") // Set it to a controllable path
	defer func() { defaultArgs.Config_file = originalDefaultPath }()
	
	configFileContent := `{
		"Start": "start_from_default_config",
		"Period": "period_from_default_config"
	}`
	if err := os.WriteFile(defaultArgs.Config_file, []byte(configFileContent), 0644); err != nil {
		t.Fatalf("Failed to write temp default config file: %v", err)
	}

	cfg := New()
	originalArgs := os.Args
	os.Args = []string{"cmd"} // No -config flag
	defer func() { os.Args = originalArgs }()

	err = cfg.Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Start != "start_from_default_config" {
		t.Errorf("Start: Expected from default config file, got %s", cfg.Start)
	}
	if cfg.Period != "period_from_default_config" {
		t.Errorf("Period: Expected from default config file, got %s", cfg.Period)
	}
	// Check actual config file path stored
	if cfg.Config_file != defaultArgs.Config_file {
		t.Errorf("Config_file path: Expected '%s', got '%s'", defaultArgs.Config_file, cfg.Config_file)
	}
}

func TestMain(m *testing.M) {
	// This TestMain is useful if we need to ensure flags are reset globally
	// or other setup/teardown for the whole package.
	// For now, individual resetFlags() should be okay.
	os.Exit(m.Run())
}

// TestConfig_Load_ConnectionStringDefault tests that ConnectionString is empty by default.
func TestConfig_Load_ConnectionStringDefault(t *testing.T) {
	resetFlags()
	os.Clearenv()

	tmpDir, _ := os.MkdirTemp("", "config-test-cs")
	defer os.RemoveAll(tmpDir)
	originalDefaultConfFile := defaultArgs.Config_file
	defaultArgs.Config_file = filepath.Join(tmpDir, "non_existent_default.cfg")
	defer func() { defaultArgs.Config_file = originalDefaultConfFile }()

	cfg := New()
	originalArgs := os.Args
	os.Args = []string{"cmd"} 
	defer func() { os.Args = originalArgs }()

	err := cfg.Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Connection_string != "" {
		t.Errorf("Connection_string: expected \"\" (empty), got \"%s\"", cfg.Connection_string)
	}
}
