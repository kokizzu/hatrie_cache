#!/usr/bin/env bash
set -euo pipefail
git add -- \
  BENCHMARK.md \
  C235_READ_WRITE_PROFILER.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  hat/hatSql/c235_read_write_profiler.go \
  hat/hatSql/c235_read_write_profiler_test.go \
  hat/hatSql/c235_read_write_profiler_baseline_benchmark_test.go \
  scripts/benchmark-c235-read-write-profiler.sh \
  scripts/commit-c235-read-write-profiler.sh \
  scripts/format-c235-read-write-profiler.sh \
  scripts/push-c235-read-write-profiler.sh \
  scripts/race-c235-read-write-profiler.sh \
  scripts/stage-c235-read-write-profiler.sh \
  scripts/test-c235-read-write-profiler.sh \
  scripts/vet-c235-read-write-profiler.sh
git diff --cached --check
git diff --cached --stat
