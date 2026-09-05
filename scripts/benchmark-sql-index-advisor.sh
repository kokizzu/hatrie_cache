#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLIndexAdvisorPersistence$' -benchmem -count=5
