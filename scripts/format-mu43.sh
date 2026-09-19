#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/sink_backpressure.go \
  hat/hatPipeline/m_u43_sink_backpressure_test.go \
  hat/hatPipeline/m_u43_sink_backpressure_benchmark_test.go
