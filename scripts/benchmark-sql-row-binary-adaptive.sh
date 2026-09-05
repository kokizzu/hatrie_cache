#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRowBinaryAdaptive$' -benchmem -count=5
