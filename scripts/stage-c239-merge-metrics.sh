#!/usr/bin/env bash
set -euo pipefail

paths=(
  BENCHMARK.md
  C239_PART_MERGE_METRICS.md
  INSPIRATION_ROUND2.md
  Makefile
  README.md
  hat/hatSql/c239_part_merge_metrics_baseline_benchmark_test.go
  hat/hatSql/c239_part_merge_metrics_benchmark_test.go
  hat/hatSql/c239_part_merge_metrics_test.go
  hat/hatSql/typed_table.go
  hat/hatSql/typed_table_part_merge_metrics.go
  hat/hatSql/typed_table_patch_parts.go
  scripts/benchmark-c239-merge-baseline.sh
  scripts/benchmark-c239-merge-before-after.sh
  scripts/commit-c239-merge-metrics.sh
  scripts/format-c239-merge-metrics.sh
  scripts/push-c239-merge-metrics.sh
  scripts/race-c239-merge-metrics.sh
  scripts/stage-c239-merge-metrics.sh
  scripts/test-c239-merge-metrics.sh
  scripts/vet-c239-merge-metrics.sh
)

printf '%s\n' 'Worktree before staging:'
git status --short
git add -- "${paths[@]}"
git diff --cached --check
printf '%s\n' 'Staged C239 change:'
git diff --cached --stat
printf '%s\n' 'Worktree after staging:'
git status --short
