#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkExecuteSQLQueryColumnarDictionaryDistinct$' -benchmem -count=5
