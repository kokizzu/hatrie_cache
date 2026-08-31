#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarDictionaryLiteralIN$' -benchmem -count=5
