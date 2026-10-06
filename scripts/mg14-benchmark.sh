#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkM14(Baseline|SinkRetryQueue)' -benchmem -count=5
