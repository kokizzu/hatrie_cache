#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCanSkipSQLRowBinaryStats$' -benchmem -count=5
