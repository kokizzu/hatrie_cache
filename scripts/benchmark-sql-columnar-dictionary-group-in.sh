#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarDictionaryGroupAggregateLiteralIN$' -benchmem -count=5
