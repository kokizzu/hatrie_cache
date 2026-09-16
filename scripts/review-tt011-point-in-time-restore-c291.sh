#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat -- \
  README.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md \
  TT011_POINT_IN_TIME_RESTORE.md \
  hat/hatBackup/reports.go \
  hat/hatCache/backup_partition_restore.go \
  hat/hatCache/backup_partition_restore_test.go \
  hat/hatCache/backup_point_in_time_restore.go \
  hat/hatCache/backup_restore.go \
  hat/hatCache/tt011_point_in_time_restore_test.go \
  hat/hatCache/tt011_restore_benchmark_test.go \
  hat/hatCache/tt011_restore_pit_benchmark_test.go \
  cmd/hatrie-cli/main.go \
  cmd/hatrie-cli/tt011_point_in_time_restore_test.go \
  scripts/test-tt011-point-in-time-restore-c291.sh \
  scripts/format-tt011-point-in-time-restore-c291.sh \
  scripts/verify-tt011-point-in-time-restore-c291.sh \
  scripts/benchmark-before-tt011-point-in-time-restore-c291.sh \
  scripts/benchmark-tt011-point-in-time-restore-c291.sh \
  scripts/review-tt011-point-in-time-restore-c291.sh \
  scripts/stage-tt011-point-in-time-restore-c291.sh \
  scripts/inspect-staged-tt011-point-in-time-restore-c291.sh \
  scripts/commit-tt011-point-in-time-restore-c291.sh \
  scripts/push-tt011-point-in-time-restore-c291.sh
