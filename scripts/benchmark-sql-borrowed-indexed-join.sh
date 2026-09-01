#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLBorrowedIndexedJoin$' -benchmem -count=5
