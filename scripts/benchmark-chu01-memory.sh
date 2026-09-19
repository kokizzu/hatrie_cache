#!/usr/bin/env bash
set -euo pipefail

mkdir -p build/benchmarks
go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkAsyncInsertDeduplicatorMemoryAccept$' -benchmem -count=5 | tee build/benchmarks/chu01-async-dedup-memory.txt
