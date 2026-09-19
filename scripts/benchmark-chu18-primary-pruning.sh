#!/usr/bin/env bash
set -euo pipefail

mkdir -p build/benchmarks
go test ./hat/hatPrimaryPruning -run '^$' -bench 'Benchmark(LinearCompositePruning|PackedCompositePruning|PackedMayOverlap|LinearCompositeFullRange|PackedCompositeFullRange|BuildPackedMarks)$' -benchmem -count=5 | tee build/benchmarks/chu18-primary-pruning.txt
