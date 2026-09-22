#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M230_SOURCE_BACKPRESSURE.md \
  Makefile \
  hat/hatReplication/m230_source_backpressure.go \
  hat/hatReplication/m230_source_backpressure_benchmark_test.go \
  hat/hatReplication/m230_source_backpressure_test.go \
  hat/hatReplication/tu39_space_changefeed.go \
  scripts/inspect-m230-docs.sh \
  scripts/m230-backpressure.sh \
  scripts/status-m230-backpressure.sh \
  scripts/stage-m230-backpressure.sh \
  scripts/commit-m230-backpressure.sh \
  scripts/push-m230-backpressure.sh
git diff --cached --check
git diff --cached --stat
