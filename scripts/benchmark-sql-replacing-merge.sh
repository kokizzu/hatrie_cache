#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLReplacingMerge$' -benchmem -count=5
