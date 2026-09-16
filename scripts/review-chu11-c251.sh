#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  CHU11_AUTOMATIC_DATA_SKIPPING_INDEX_SELECTION.md
  Makefile
  PRODUCT_IDEA_GAPS.md
  README.md
  hat/hatSql/ch_u11_skip_index_advisor_benchmark_test.go
  hat/hatSql/ch_u11_skip_index_advisor_test.go
  hat/hatSql/index_advisor.go
  hat/hatSql/index_advisor_persistence.go
  hat/hatSql/index_advisor_persistence_test.go
  scripts/benchmark-chu11-before-c251.sh
  scripts/benchmark-chu11-c251.sh
  scripts/commit-chu11-c251.sh
  scripts/format-chu11-c251.sh
  scripts/push-chu11-c251.sh
  scripts/race-chu11-clean-c251.sh
  scripts/review-chu11-c251.sh
  scripts/stage-chu11-c251.sh
  scripts/test-chu11-c251.sh
  scripts/test-chu11-clean-c251.sh
  scripts/test-chu11-package-clean-c251.sh
  scripts/verify-chu11-docs-c251.sh
  scripts/vet-chu11-clean-c251.sh
)

printf '%s\n' '--- CH-U11 tracked diff ---'
git diff --stat -- "${feature_paths[@]}"
printf '%s\n' '--- CH-U11 whitespace check ---'
git diff --check -- "${feature_paths[@]}"
printf '%s\n' '--- worktree status (unrelated changes preserved) ---'
git status --short
