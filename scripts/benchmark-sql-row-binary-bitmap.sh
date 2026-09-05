#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRowBinaryBitmap$' -benchmem -count=5
