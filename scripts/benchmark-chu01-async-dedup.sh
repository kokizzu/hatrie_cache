#!/usr/bin/env bash
set -euo pipefail

mkdir -p build/benchmarks
go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkAsyncInsert(DeduplicatorMemoryAccept|DedupFileAppend)$' -benchmem -count=5 | tee build/benchmarks/chu01-async-dedup.txt
