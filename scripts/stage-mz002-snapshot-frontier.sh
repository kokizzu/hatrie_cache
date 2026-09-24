#!/usr/bin/env bash
set -euo pipefail

git add \
  ENGINE_IDEAS.md \
  Makefile \
  MZ002_SNAPSHOT_FRONTIER.md \
  hat/hatPipeline/mz002_snapshot_frontier.go \
  hat/hatPipeline/mz002_snapshot_frontier_baseline_test.go \
  hat/hatPipeline/mz002_snapshot_frontier_test.go \
  scripts/benchmark-mz002-snapshot-frontier.sh \
  scripts/format-mz002-snapshot-frontier.sh \
  scripts/test-mz002-snapshot-frontier.sh \
  scripts/verify-mz002-snapshot-frontier.sh \
  scripts/stage-mz002-snapshot-frontier.sh \
  scripts/commit-mz002-snapshot-frontier.sh \
  scripts/push-mz002-snapshot-frontier.sh
