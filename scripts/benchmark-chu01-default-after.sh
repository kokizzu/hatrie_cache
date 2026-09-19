#!/usr/bin/env bash
set -euo pipefail

mkdir -p build/benchmarks
go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkAsyncBatcher(Submit|SubmitMaxOne)$' -benchmem -count=5 | tee build/benchmarks/chu01-default-after.txt
