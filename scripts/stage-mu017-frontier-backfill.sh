#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  Makefile \
  MU017_FRONTIER_BACKFILL.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/incremental_projection.go \
  hat/hatSql/mu017_frontier_backfill.go \
  hat/hatSql/mu017_frontier_backfill_test.go \
  hat/hatSql/mu017_frontier_backfill_baseline_benchmark_test.go \
  hat/hatSql/mu017_frontier_backfill_benchmark_test.go \
  scripts/benchmark-mu017-baseline.sh \
  scripts/benchmark-mu017-frontier-backfill.sh \
  scripts/commit-mu017-frontier-backfill.sh \
  scripts/format-mu017-frontier-backfill.sh \
  scripts/push-mu017-frontier-backfill.sh \
  scripts/race-mu017-frontier-backfill.sh \
  scripts/stage-mu017-frontier-backfill.sh \
  scripts/test-mu017-frontier-backfill-package.sh \
  scripts/test-mu017-frontier-backfill.sh \
  scripts/vet-mu017-frontier-backfill.sh
git diff --cached --check
git diff --cached --name-status
