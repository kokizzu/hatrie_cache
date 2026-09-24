#!/usr/bin/env bash
set -euo pipefail

benchtime="${BENCHTIME:-200ms}"
count="${COUNT:-5}"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRuntimeJoinFilter$' -benchmem -benchtime="$benchtime" -count="$count" "$@"
