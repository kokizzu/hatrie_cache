#!/usr/bin/env bash
set -euo pipefail

mkdir -p build/benchmarks
go test ./hat/hatMembership -run '^$' -bench 'Benchmark(LinearContainsSmall|AdaptiveSortedContains|AdaptiveBitmapContains|AdaptiveHashContains|BuildDenseBitmap|BuildSparseHash)$' -benchmem -count=5 | tee build/benchmarks/chu17-membership.txt
