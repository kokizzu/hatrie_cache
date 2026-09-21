#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestSQLJSONPathSkipIndexExplainDiagnostics$' -count=1
go test -race ./hat/hatCache -run '^TestSQLJSONPathSkipIndexExplainDiagnostics$' -count=1
go test ./hat/hatCache -run '^$' -bench '^BenchmarkCHU49Skip' -benchmem -count="${COUNT:-5}" -benchtime="${BENCHTIME:-200ms}"
