#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION.md \
  BENCHMARK.md \
  CHU28_DISK_IO_THROTTLING.md \
  hat/hatStorage/compaction_scheduler.go \
  hat/hatStorage/ch_u28_legacy_fastpath_test.go \
  scripts/benchmark-chu28-standalone.sh \
  scripts/format-chu28-standalone.sh \
  scripts/race-chu28-standalone.sh \
  scripts/test-chu28-standalone.sh \
  scripts/vet-chu28-standalone.sh \
  scripts/verify-chu28-fastpath.sh \
  scripts/stage-chu28-fastpath.sh \
  scripts/commit-chu28-fastpath.sh \
  scripts/push-chu28-fastpath.sh

git diff --cached --name-only
