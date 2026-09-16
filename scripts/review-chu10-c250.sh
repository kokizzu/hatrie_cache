#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --stat -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CHU10_FEEDBACK_PROJECTION_SELECTION.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/projection_advisor.go \
  hat/hatSql/ch_u10_projection_feedback_benchmark_test.go \
  hat/hatSql/ch_u10_projection_feedback_integration_test.go \
  hat/hatSql/ch_u10_projection_feedback_test.go \
  scripts/benchmark-chu10-before-c250.sh \
  scripts/benchmark-chu10-c250.sh \
  scripts/format-chu10-c250.sh \
  scripts/race-chu10-c250.sh \
  scripts/review-chu10-c250.sh \
  scripts/test-chu10-c250.sh \
  scripts/test-chu10-package-c250.sh \
  scripts/test-chu10-repo-c250.sh \
  scripts/verify-chu10-docs-c250.sh \
  scripts/vet-chu10-c250.sh
git status --short
