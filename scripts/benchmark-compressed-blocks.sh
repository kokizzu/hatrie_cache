#!/usr/bin/env bash
set -euo pipefail

count="${BENCH_COUNT:-5}"
go test ./hat/hatCodec -run '^$' -bench 'Benchmark(CompressedBlocks|Gzip)' -benchmem -count="$count"
