#!/usr/bin/env bash
set -euo pipefail

rg -n -C 3 'C250|C204' INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -n 'AsyncInsertDedup|AsyncInsertDeduplicator' \
  hat/hatPipeline/async_insert_dedup.go \
  hat/hatPipeline/async_insert_dedup_test.go \
  hat/hatPipeline/async_insert_dedup_benchmark_test.go
rg -n 'C249|C250|async insert|AsyncInsert' README.md BENCHMARK.md
rg -n -C 12 '^## C249' BENCHMARK.md
rg -n 'test-chu01-async-dedup|benchmark-chu01-async-dedup|race-chu01-async-dedup|vet-chu01-async-dedup' Makefile
git log --oneline --all -- hat/hatPipeline/async_insert_dedup.go
