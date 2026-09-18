#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ020_TWO_PHASE_SINK_CHECKPOINT.md \
  hat/hatSql/mz020_two_phase_sink_checkpoint.go \
  hat/hatSql/mz020_two_phase_sink_checkpoint_test.go \
  hat/hatSql/mz020_two_phase_sink_checkpoint_benchmark_test.go \
  scripts/format-mz020-two-phase-sink.sh \
  scripts/test-mz020-two-phase-sink.sh \
  scripts/test-mz020-two-phase-sink-package.sh \
  scripts/benchmark-mz020-two-phase-sink.sh \
  scripts/race-mz020-two-phase-sink.sh \
  scripts/vet-mz020-two-phase-sink.sh \
  scripts/verify-mz020-two-phase-sink-docs.sh \
  scripts/stage-mz020-two-phase-sink.sh \
  scripts/commit-mz020-two-phase-sink.sh \
  scripts/push-mz020-two-phase-sink.sh

git status --short
