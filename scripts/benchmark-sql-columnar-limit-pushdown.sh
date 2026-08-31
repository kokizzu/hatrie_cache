#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLColumnarStreamMaterializeLimit$' -benchmem -count=5
