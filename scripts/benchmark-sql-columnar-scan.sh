#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnar(DictionaryFilter|NumericAggregate|NumericFilter|Scan)$' -benchmem -count=1
