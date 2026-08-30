#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLHatTrieBorrowedColumnarLayout$' -benchmem -count=5
