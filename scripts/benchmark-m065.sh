#!/usr/bin/env bash
set -euo pipefail
cache_dir="/tmp/hatrie-cache-m065-gocache"
mkdir -p "$cache_dir"
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkM065(DifferentialRowNumberLagTail|FullRebuildCorrectionTail|DifferentialRowNumberLagFront|FullRebuildCorrectionFront)$' -benchmem -benchtime=100ms -count=5 | tee M065_BENCHMARK_RAW.txt
sed -i 's/[[:space:]]*$//' M065_BENCHMARK_RAW.txt
