#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M234_SINK_BACKPRESSURE.md \
  Makefile \
  hat/hatReplication/m233_sink_retry.go \
  hat/hatReplication/m234_sink_backpressure.go \
  hat/hatReplication/m234_sink_backpressure_test.go \
  hat/hatReplication/m234_sink_backpressure_benchmark_test.go \
  scripts/m234-sink-backpressure.sh \
  scripts/stage-m234-sink-backpressure.sh \
  scripts/commit-m234-sink-backpressure.sh \
  scripts/push-m234-sink-backpressure.sh
