#!/usr/bin/env bash
set -euo pipefail

count="${BENCH_COUNT:-5}"
go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQLRowBinary(DictionaryEncode|DictionaryDecode|EncodeBaseline|DecodeBaseline)' -benchmem -count="$count"
