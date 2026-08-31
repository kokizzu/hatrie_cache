#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarDictionaryDistinctLiteralIN$' -benchmem -count=5
