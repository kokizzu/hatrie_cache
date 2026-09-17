#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  Makefile \
  MU016_TRANSACTIONAL_VIEW_DDL.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/session.go \
  hat/hatSql/mu016_transactional_view.go \
  hat/hatSql/mu016_transactional_view_test.go \
  hat/hatSql/mu016_transactional_view_baseline_benchmark_test.go \
  hat/hatSql/mu016_transactional_view_benchmark_test.go \
  scripts/benchmark-mu016-baseline.sh \
  scripts/benchmark-mu016-transactional-views.sh \
  scripts/commit-mu016-transactional-views.sh \
  scripts/format-mu016-transactional-views.sh \
  scripts/push-mu016-transactional-views.sh \
  scripts/race-mu016-transactional-views.sh \
  scripts/stage-mu016-transactional-views.sh \
  scripts/test-mu016-transactional-views.sh \
  scripts/test-mu016-transactional-view-package.sh \
  scripts/vet-mu016-transactional-views.sh
git diff --cached --check
git diff --cached --name-status
