#!/usr/bin/env bash
set -euo pipefail

benchtime="${BENCHTIME:-200ms}"
count="${BENCH_COUNT:-5}"
go test ./hat/hatBackup -run '^$' -bench '^BenchmarkMZ006' -benchmem -benchtime="$benchtime" -count="$count"
