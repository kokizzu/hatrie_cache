#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQLRowBinaryDecimal(StringBaseline|128Typed|256Typed)(Encode|Decode)$' -benchmem -count=5
