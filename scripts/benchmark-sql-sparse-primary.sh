#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLColumnarSparsePrimaryRange$' -benchmem -count=5
