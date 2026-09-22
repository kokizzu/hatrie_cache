#!/usr/bin/env bash
set -euo pipefail

paths=(
  BENCHMARK.md
  C238_MUTATION_QUEUE_PROGRESS.md
  INSPIRATION_ROUND2.md
  Makefile
  README.md
  hat/hatSql/c238_mutation_queue_progress_test.go
  hat/hatSql/c238_mutation_queue_progress_baseline_benchmark_test.go
  hat/hatSql/c238_mutation_queue_progress_benchmark_test.go
  hat/hatSql/mutation_dependency_graph.go
  hat/hatSql/sql_mutation_dependency_queue.go
  scripts/benchmark-c238-baseline.sh
  scripts/benchmark-c238-before-after.sh
  scripts/format-c238-progress.sh
  scripts/race-c238-progress.sh
  scripts/stage-c238-progress.sh
  scripts/test-c238-progress.sh
  scripts/vet-c238-progress.sh
  scripts/commit-c238-progress.sh
  scripts/push-c238-progress.sh
)

printf '%s\n' 'Worktree before staging:'
git status --short
git add -- "${paths[@]}"
git diff --cached --check
printf '%s\n' 'Staged C238 change:'
git diff --cached --stat
printf '%s\n' 'Worktree after staging:'
git status --short
