#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  Makefile \
  MU035_SNAPSHOT_BLOCKING.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatPipeline/mu035_snapshot_blocking_benchmark_test.go \
  hat/hatPipeline/mu035_snapshot_blocking_test.go \
  hat/hatPipeline/mz010_snapshot_cutover.go \
  scripts/benchmark-mu035-snapshot-blocking.sh \
  scripts/format-mu035-snapshot-blocking.sh \
  scripts/verify-mu035-snapshot-blocking.sh \
  scripts/stage-mu035-snapshot-blocking.sh
git diff --cached --check
git diff --cached --stat
