#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarDictionaryLiteralINNumericConjunction$' -benchmem -count=5
