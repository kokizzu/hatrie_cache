#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat -- \
  Makefile \
  README.md \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ047_TIMELINE_RECOVERY.md \
  hat/hatPipeline/mz047_timeline_recovery.go \
  hat/hatPipeline/mz047_timeline_recovery_test.go \
  scripts/format-mz047-timeline-recovery.sh \
  scripts/benchmark-mz047-timeline-recovery.sh \
  scripts/test-mz047-timeline-recovery.sh \
  scripts/test-mz047-package.sh \
  scripts/race-mz047-timeline-recovery.sh \
  scripts/vet-mz047-timeline-recovery.sh \
  scripts/review-mz047.sh \
  scripts/stage-mz047.sh \
  scripts/commit-mz047.sh \
  scripts/push-mz047.sh
