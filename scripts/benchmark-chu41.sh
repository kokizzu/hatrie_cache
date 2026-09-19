#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^(BenchmarkCHU41|BenchmarkSQLPreparedQueryCache)' -benchmem -count=5
