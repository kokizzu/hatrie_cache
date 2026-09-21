#!/usr/bin/env bash
set -euo pipefail

count="${COUNT:-5}"
benchtime="${BENCHTIME:-200ms}"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkC233CPUCheck' -benchmem -count="$count" -benchtime="$benchtime"
