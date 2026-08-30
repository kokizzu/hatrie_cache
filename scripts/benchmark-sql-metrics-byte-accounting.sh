#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLMetricsDisabledFilteredQuery$' -benchmem -count=5
