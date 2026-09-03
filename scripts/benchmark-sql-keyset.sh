#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQLKeysetPaginationDeepPage' -benchmem -count=5
