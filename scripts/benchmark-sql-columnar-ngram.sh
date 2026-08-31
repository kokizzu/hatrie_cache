#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkColumnarNGramLikeFilter$' -benchmem -count=3
