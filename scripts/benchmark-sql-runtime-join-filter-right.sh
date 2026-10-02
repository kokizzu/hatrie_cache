#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRuntimeJoinFilter/selective_right_where_100k_left_512_right/' -benchmem -count=3 -benchtime=20x
