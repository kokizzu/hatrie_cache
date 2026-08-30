#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLColumnarSelectiveFilter' -benchmem -count=5
