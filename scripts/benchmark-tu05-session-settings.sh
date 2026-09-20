#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLSessionExecute(DefaultSettings|TimeoutSettings)$' -benchmem -count=5
