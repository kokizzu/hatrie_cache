#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLProjectionAdvisor$' -benchmem -count=3
