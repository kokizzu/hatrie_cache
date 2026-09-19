#!/usr/bin/env bash
set -euo pipefail

mkdir -p build/benchmarks
go test ./hat/hatSpill -run '^$' -bench 'Benchmark(DirectAtomicSpillCounter|BudgetReserveRelease|BudgetTryReserveRelease|BudgetSnapshot)$' -benchmem -count=5 | tee build/benchmarks/chu25-spill-quota.txt
