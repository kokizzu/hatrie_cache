#!/usr/bin/env bash
set -euo pipefail

mkdir -p build/benchmarks
go test ./hat/hatCache -run '^$' -bench '^BenchmarkMZ022' -benchmem -count=5 | tee build/benchmarks/mz022-index-readiness.txt
