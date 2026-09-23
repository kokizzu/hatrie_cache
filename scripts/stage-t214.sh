#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  T214_SNAPSHOT_STREAMING.md \
  hat/hatCache/snapshot.go \
  hat/hatCache/snapshot_restore_staged.go \
  hat/hatCache/t214_snapshot_stream_baseline_benchmark_test.go \
  hat/hatCache/t214_snapshot_stream_benchmark_test.go \
  hat/hatCache/t214_snapshot_stream_test.go \
  scripts/benchmark-t214-before.sh \
  scripts/benchmark-t214.sh \
  scripts/commit-t214.sh \
  scripts/format-t214.sh \
  scripts/push-t214.sh \
  scripts/race-t214.sh \
  scripts/stage-t214.sh \
  scripts/test-t214-package.sh \
  scripts/test-t214.sh \
  scripts/verify-t214-scope.sh \
  scripts/vet-t214.sh
