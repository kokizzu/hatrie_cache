#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkC202(GlobalAsyncBatcherSetup|PartitionedAsyncBatcherSetup)$' -benchmem -count=5 -benchtime=200ms -cpu=1
