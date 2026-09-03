#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench 'BenchmarkExplainSQLWhatIf' -benchmem -count=5
