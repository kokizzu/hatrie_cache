#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQLMultiSourceSnapshot' -benchmem -count=5
