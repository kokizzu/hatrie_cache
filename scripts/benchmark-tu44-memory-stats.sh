#!/usr/bin/env bash
set -euo pipefail

mkdir -p build/benchmarks
go test ./hat/hatMemoryStats -run '^$' -bench '^Benchmark(Compute|Snapshot)$' -benchmem -count=5 | tee build/benchmarks/tu44-memory-stats.txt
