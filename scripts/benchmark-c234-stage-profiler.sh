#!/usr/bin/env bash
set -euo pipefail

count="${COUNT:-5}"
benchtime="${BENCHTIME:-200ms}"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH234StageProfiler' -benchmem -count="$count" -benchtime="$benchtime"
