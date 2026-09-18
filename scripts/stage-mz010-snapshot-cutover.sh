#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  Makefile \
  MZ010_SNAPSHOT_CUTOVER.md \
  README.md \
  hat/hatPipeline/mz010_snapshot_cutover.go \
  hat/hatPipeline/mz010_snapshot_cutover_benchmark_test.go \
  hat/hatPipeline/mz010_snapshot_cutover_test.go \
  scripts/benchmark-mz010-snapshot-cutover.sh \
  scripts/commit-mz010-snapshot-cutover.sh \
  scripts/format-mz010-snapshot-cutover.sh \
  scripts/push-mz010-snapshot-cutover.sh \
  scripts/race-mz010-snapshot-cutover.sh \
  scripts/review-mz010-snapshot-cutover.sh \
  scripts/stage-mz010-snapshot-cutover.sh \
  scripts/test-mz010-package.sh \
  scripts/test-mz010-snapshot-cutover.sh \
  scripts/vet-mz010-snapshot-cutover.sh
git diff --cached --check
git diff --cached --name-status
