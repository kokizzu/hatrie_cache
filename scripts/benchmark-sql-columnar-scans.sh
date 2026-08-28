#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarScan$' -benchmem -count=3
