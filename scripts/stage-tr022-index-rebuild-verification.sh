#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  Makefile \
  README.md \
  TR022_INDEX_REBUILD_VERIFICATION.md \
  hat/hatSql/index_rebuild_queue.go \
  hat/hatSql/tr022_index_rebuild_verification_benchmark_test.go \
  hat/hatSql/tr022_index_rebuild_verification_public_test.go \
  hat/hatSql/tr022_index_rebuild_verification_test.go \
  scripts/benchmark-tr022-index-rebuild-verification.sh \
  scripts/commit-tr022-index-rebuild-verification.sh \
  scripts/format-tr022-index-rebuild-verification.sh \
  scripts/push-tr022-index-rebuild-verification.sh \
  scripts/race-tr022-index-rebuild-verification.sh \
  scripts/review-tr022.sh \
  scripts/stage-tr022-index-rebuild-verification.sh \
  scripts/test-tr022-index-rebuild-verification.sh \
  scripts/test-tr022-package.sh \
  scripts/vet-tr022-index-rebuild-verification.sh
