#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarNumericAggregateDictionaryLiteralIN$' -benchmem -count=5
