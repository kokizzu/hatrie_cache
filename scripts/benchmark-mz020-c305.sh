#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline \
  -run '^$' \
  -bench 'Benchmark(FixedSchedulerNoopBatch|ResizableSchedulerNoopBatch|ResizableSchedulerResize)$' \
  -benchmem \
  -count=5
