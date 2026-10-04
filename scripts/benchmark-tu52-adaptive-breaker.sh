#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
cache_dir=${GOCACHE:-$root/build/go-cache/tu52-adaptive-breaker}
mkdir -p "$root/$artifact_dir" "$cache_dir"
GOCACHE="$cache_dir" go -C "$root" test ./hat/hatReplication -run '^$' -bench '^BenchmarkTU52' -benchmem -benchtime="${BENCHTIME:-200ms}" -count="${BENCHCOUNT:-5}" | tee "$root/$artifact_dir/tu52-adaptive-breaker.txt"
