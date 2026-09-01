#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableDictionaryStringStorage$' -benchmem -count=5
