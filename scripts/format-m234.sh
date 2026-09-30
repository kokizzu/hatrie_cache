#!/bin/sh
set -eu

cd /tmp/hatrie-cache-m234
gofmt -w hat/hatPipeline/m234_sink_pending_output.go hat/hatPipeline/m234_sink_pending_output_test.go hat/hatPipeline/m234_sink_pending_output_benchmark_test.go
