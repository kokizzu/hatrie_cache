#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M231_EXACTLY_ONCE_UPSERT_SINK.md \
  Makefile \
  hat/hatReplication/m231_exactly_once_upsert_sink.go \
  hat/hatReplication/m231_exactly_once_upsert_sink_benchmark_test.go \
  hat/hatReplication/m231_exactly_once_upsert_sink_test.go \
  scripts/m231-sink.sh \
  scripts/status-m231-sink.sh \
  scripts/stage-m231-sink.sh \
  scripts/commit-m231-sink.sh \
  scripts/push-m231-sink.sh
git diff --cached --check
git diff --cached --stat
