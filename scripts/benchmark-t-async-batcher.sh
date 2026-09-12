#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkAsyncBatcherSubmit(MaxOne)?$' -benchmem -count=5
