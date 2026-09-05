#!/usr/bin/env bash
set -euo pipefail

count="${COUNT:-5}"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLQueryManager/' -benchmem -count="$count"
