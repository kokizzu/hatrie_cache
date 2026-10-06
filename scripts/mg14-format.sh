#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPipeline/m14_sink_retry_queue.go hat/hatPipeline/m14_sink_retry_queue_test.go hat/hatPipeline/m14_sink_retry_queue_benchmark_test.go
