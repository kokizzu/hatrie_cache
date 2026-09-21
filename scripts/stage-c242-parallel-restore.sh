#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C242_PARALLEL_RESTORE.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  cmd/hatrie-cli/c242_parallel_restore_test.go \
  cmd/hatrie-cli/main.go \
  hat/hatBackup/reports.go \
  hat/hatBackup/restore_files.go \
  hat/hatBackup/restore_files_c242_benchmark_test.go \
  hat/hatBackup/restore_files_c242_test.go \
  hat/hatCache/backup_repository.go \
  hat/hatCache/backup_restore.go \
  scripts/benchmark-c242-parallel-restore.sh \
  scripts/commit-c242-parallel-restore.sh \
  scripts/format-c242-parallel-restore.sh \
  scripts/push-c242-parallel-restore.sh \
  scripts/race-c242-parallel-restore.sh \
  scripts/review-c242-parallel-restore.sh \
  scripts/stage-c242-parallel-restore.sh \
  scripts/test-c242-cli.sh \
  scripts/test-c242-package.sh \
  scripts/test-c242-parallel-restore.sh \
  scripts/vet-c242-parallel-restore.sh

git diff --cached --check
