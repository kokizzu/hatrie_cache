#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarDictionaryGroupAggregateWithoutOrder$' -benchmem -count=5
