#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  Makefile \
  MZ050_TIMELINE_BRANCH.md \
  README.md \
  hat/hatCache/mz050_timeline_branch.go \
  hat/hatCache/mz050_timeline_branch_benchmark_test.go \
  hat/hatCache/mz050_timeline_branch_test.go \
  scripts/benchmark-mz050-timeline-branch.sh \
  scripts/commit-mz050-timeline-branch.sh \
  scripts/format-mz050-timeline-branch.sh \
  scripts/inspect-inspiration-backlog.sh \
  scripts/push-mz050-timeline-branch.sh \
  scripts/race-mz050-timeline-branch.sh \
  scripts/stage-mz050-timeline-branch.sh \
  scripts/test-mz050-timeline-branch.sh \
  scripts/verify-mz050-timeline-branch.sh

git diff --cached --check
git status --short
