#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M233_SINK_RETRY_OUTBOX.md \
  Makefile \
  hat/hatReplication/m233_sink_retry.go \
  hat/hatReplication/m233_sink_retry_benchmark_test.go \
  hat/hatReplication/m233_sink_retry_test.go \
  scripts/inventory-hatrie-tmp.sh \
  scripts/m233-sink-retry.sh \
  scripts/verify-m233-sink-retry.sh \
  scripts/stage-m233-sink-retry.sh \
  scripts/commit-m233-sink-retry.sh \
  scripts/push-m233-sink-retry.sh
