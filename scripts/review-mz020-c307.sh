#!/usr/bin/env bash
set -euo pipefail

paths=(
  hat/hatPipeline/mz020_resizable_scheduler.go
  hat/hatPipeline/mz020_resizable_scheduler_test.go
  hat/hatPipeline/mz020_resizable_scheduler_benchmark_test.go
  scripts/format-mz020-c305.sh
  scripts/test-mz020-c305.sh
  scripts/test-race-mz020-c305.sh
  scripts/vet-mz020-c305.sh
  scripts/benchmark-mz020-c305.sh
  scripts/review-mz020-c307.sh
  scripts/stage-mz020-c307.sh
  scripts/inspect-staged-mz020-c307.sh
  scripts/commit-mz020-c307.sh
  scripts/push-mz020-c307.sh
  MZ020_RESIZABLE_SCHEDULER.md
  README.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
)

git diff --check -- "${paths[@]}"
git status --short -- "${paths[@]}" Makefile
git diff --stat -- "${paths[@]}"
