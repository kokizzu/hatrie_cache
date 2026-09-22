#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  README.md \
  T214_STREAMING_SNAPSHOTS.md \
  hat/hatCache/journal_pull.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/t214_snapshot_stream_test.go \
  hat/hatCache/t214_snapshot_stream_baseline_benchmark_test.go \
  hat/hatCache/t214_snapshot_stream_benchmark_test.go \
  hat/hatCache/t214_snapshot_stream_benchmark_helpers_test.go \
  scripts/test-t214.sh \
  scripts/test-t214-package.sh \
  scripts/benchmark-t214-baseline.sh \
  scripts/benchmark-t214.sh \
  scripts/format-t214.sh \
  scripts/race-t214.sh \
  scripts/vet-t214.sh \
  scripts/verify-docs-t214.sh \
  scripts/stage-t214.sh \
  scripts/commit-t214.sh \
  scripts/push-t214.sh
git diff --cached --check
git diff --cached --stat
