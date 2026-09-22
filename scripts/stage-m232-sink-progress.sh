#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M232_SINK_PROGRESS_ENVELOPES.md \
  Makefile \
  hat/hatReplication/m232_sink_progress.go \
  hat/hatReplication/m232_sink_progress_benchmark_test.go \
  hat/hatReplication/m232_sink_progress_test.go \
  scripts/m232-sink-progress.sh \
  scripts/status-m232-sink-progress.sh \
  scripts/stage-m232-sink-progress.sh \
  scripts/commit-m232-sink-progress.sh \
  scripts/push-m232-sink-progress.sh
git diff --cached --check
git diff --cached --stat
