#!/usr/bin/env bash
set -euo pipefail

git diff --check
printf '%s\n' 'Worktree status:'
git status --short
printf '%s\n' 'Feature diff stat:'
git diff --stat -- \
  README.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  hat/hatBackup/model.go \
  hat/hatCache/backup_partition_restore.go \
  hat/hatCache/backup_partition_restore_benchmark_test.go \
  hat/hatCache/backup_partition_restore_test.go \
  hat/hatCache/backup_restore.go \
  hat/hatCache/backup_selective_test.go \
  Makefile \
  scripts/format-selective-restore-c224.sh \
  scripts/run-selective-restore-c224.sh
