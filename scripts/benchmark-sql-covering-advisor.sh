#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLIndexAdvisorCovering' -benchmem -count=5
