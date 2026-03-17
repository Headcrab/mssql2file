# AGENTS.md

## Scope

These instructions apply to the whole repository.

## Project Summary

`mssql2file` is a CLI exporter that reads rows from a database and writes period-based files in multiple formats with optional compression.

Supported database drivers in the current code:
- `mssql`
- `mysql`
- `clickhouse`

Supported output formats in the current code:
- `json`
- `csv`
- `xml`

Supported compression modes in the current code:
- `none`
- `gz`
- `lz4`

## Stack And Tooling

- Go toolchain: `1.25.x` (`go.mod` currently targets `go 1.25.0`)
- Task runner: `task`
- Swagger generation: `swag`
- Tests: standard `go test`

## Repository Map

- `cmd/main.go`: CLI entrypoint and top-level process exit behavior
- `internal/app`: app bootstrap, config load, exporter startup
- `internal/config`: flag/env/config-file loading and precedence
- `internal/exporter`: main export flow, DB query execution, file naming, persistence of last processed period
- `internal/format`: output encoders
- `internal/compressor`: gzip/lz4/no-op compressors
- `internal/file`: file backends
- `docs`: generated swagger artifacts, do not edit manually
- `taskfile.yml`: canonical task definitions
- `.env`: tracked project metadata used by `task`

## Working Rules

- Prefer `rg` for search.
- Use `apply_patch` for manual file edits.
- Do not hand-edit files in `docs/`; regenerate them through `task swagger` or `task build`.
- Treat `.env` as tracked project metadata, not as a secret store.
- Do not silently bump `PROJECT_VERSION`; use `task update_version` only when a version bump is intentional.

## Verification Commands

Run these after code changes unless the task explicitly does not need them:

```powershell
go test ./...
task test
task build
```

Useful direct checks:

```powershell
.\bin\mssql2file.exe -h
```

## Repo-Specific Gotchas

- `task build` produces `bin/mssql2file.exe` for Windows and also regenerates swagger files.
- `task build` is expected to be side-effect free for versioning; version bumps are a separate `task update_version`.
- Config precedence is not the common `flags > env > file > defaults`.
  The current implementation and tests encode: `flags > config file > defaults > env`.
  If you change that behavior, update both code and tests in `internal/config/config_test.go`.
- Help handling is split:
  `internal/config` returns a help-style error for `-h`, and `cmd/main.go` suppresses error logging for that path.
- `internal/compressor/lz4.go` uses `github.com/pierrec/lz4/v4`; keep imports on the versioned module path.

## Change Guidance

- If you touch CLI flags, config defaults, or merge precedence:
  update `internal/config/config.go` and `internal/config/config_test.go` together.
- If you touch exporter flow or file naming:
  run tests in `internal/exporter` at minimum, then run full `go test ./...`.
- If you touch build metadata:
  check `.env`, `taskfile.yml`, and `cmd/main.go` together so ldflags still map to real variables.
- If you add a new output format or compression type:
  register it in the corresponding package and cover it with tests where practical.

## Commit Guidance

- Keep commits logical and narrow.
- Separate dependency/toolchain updates from behavior changes and from documentation changes when possible.
