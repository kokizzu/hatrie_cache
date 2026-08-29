#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQL(Columnar(DictionaryFilter|NumericAggregate|NumericFilter|Scan)|JSONColumnarBatch)$' -benchmem -count=1
