#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkAnalyzeSQLRowBinaryRead$' -benchmem -count=5
