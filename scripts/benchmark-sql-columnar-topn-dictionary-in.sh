#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarTopNDictionaryLiteralIN$' -benchmem -count=5
