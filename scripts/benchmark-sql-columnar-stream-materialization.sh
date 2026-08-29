#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnar(NumericFilterLimited|DictionaryFilter)$' -benchmem -count=1
