#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLColumnarDictionaryGroupAggregate$' -benchmem -count=5
