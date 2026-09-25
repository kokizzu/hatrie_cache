#!/usr/bin/env bash
set -euo pipefail
git add \
  ENGINE_IDEAS.md \
  MZ003_HYDRATION_PROGRESS.md \
  Makefile \
  hat/hatCache/mz003_snapshot_restore_progress_baseline_test.go \
  hat/hatCache/mz003_snapshot_restore_progress_benchmark_test.go \
  hat/hatCache/mz003_snapshot_restore_progress_test.go \
  hat/hatCache/snapshot_restore_progress.go \
  hat/hatCache/snapshot_restore_staged.go \
  scripts/benchmark-mz003-snapshot-progress-baseline.sh \
  scripts/benchmark-mz003-snapshot-progress.sh \
  scripts/format-mz003-snapshot-progress.sh \
  scripts/stage-mz003-snapshot-progress.sh \
  scripts/test-mz003-snapshot-progress.sh \
  scripts/verify-mz003-snapshot-progress.sh \
  scripts/commit-mz003-snapshot-progress.sh \
  scripts/push-mz003-snapshot-progress.sh
git diff --cached --stat
