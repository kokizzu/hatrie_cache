#!/usr/bin/env bash
set -euo pipefail

benchtime="${BENCHTIME:-200ms}"
count="${BENCH_COUNT:-5}"
go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkMZ003' -benchmem -benchtime="$benchtime" -count="$count"
