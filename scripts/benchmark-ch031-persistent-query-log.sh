#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^(BenchmarkSQLQueryManagerHistoryAppendSampling|BenchmarkCH031SQLQueryManagerDefaultExecute|BenchmarkCH031SQLQueryLogAppend)$' -benchmem -benchtime=1s -count=5
