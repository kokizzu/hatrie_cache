#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C240_READ_ONLY_BACKUP_ATTACHMENT.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  hat/hatCache/backup_attachment.go \
  hat/hatCache/c240_backup_attachment_baseline_benchmark_test.go \
  hat/hatCache/c240_backup_attachment_test.go \
  scripts/benchmark-c240-backup-attachment-baseline.sh \
  scripts/benchmark-c240-backup-attachment.sh \
  scripts/format-c240-backup-attachment.sh \
  scripts/race-c240-backup-attachment.sh \
  scripts/stage-c240-backup-attachment.sh \
  scripts/commit-c240-backup-attachment.sh \
  scripts/push-c240-backup-attachment.sh \
  scripts/test-c240-backup-attachment.sh \
  scripts/vet-c240-backup-attachment.sh

git diff --cached --check
git diff --cached --stat
