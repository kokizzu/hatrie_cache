#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarDictionaryGroupSegmentSkip$' -benchmem -count=5
