#!/usr/bin/env bash
set -euo pipefail

files=(
  BENCHMARK.md
  MU012_ARRANGEMENT_EXPLAIN.md
  PRODUCT_IDEA_GAPS.md
  README.md
  Makefile
  hat/hatSql/catalog.go
  hat/hatSql/explain_dataflow.go
  hat/hatSql/explain_pipeline.go
  hat/hatSql/materialized.go
  hat/hatSql/model.go
  hat/hatSql/mu012_arrangement_explain.go
  hat/hatSql/mu012_arrangement_explain_benchmark_test.go
  hat/hatSql/mu012_arrangement_explain_test.go
  hat/hatSql/query.go
  hat/hatSql/result_cache.go
  scripts/benchmark-mu012-arrangement-explain.sh
  scripts/format-mu012-arrangement-explain.sh
  scripts/race-mu012-arrangement-explain.sh
  scripts/review-mu012-arrangement-explain.sh
  scripts/stage-mu012-arrangement-explain.sh
  scripts/commit-mu012-arrangement-explain.sh
  scripts/push-mu012-arrangement-explain.sh
  scripts/test-mu012-arrangement-explain.sh
  scripts/test-mu012-arrangement-package.sh
  scripts/vet-mu012-arrangement-explain.sh
)

git status --short
git diff --check -- "${files[@]}"
git diff --cached --check -- "${files[@]}"
git diff --stat -- "${files[@]}"
git diff --cached --stat -- "${files[@]}"
git diff --cached --unified=3 -- "${files[@]}"
bash -n \
  scripts/benchmark-mu012-arrangement-explain.sh \
  scripts/format-mu012-arrangement-explain.sh \
  scripts/race-mu012-arrangement-explain.sh \
  scripts/review-mu012-arrangement-explain.sh \
  scripts/stage-mu012-arrangement-explain.sh \
  scripts/commit-mu012-arrangement-explain.sh \
  scripts/push-mu012-arrangement-explain.sh \
  scripts/test-mu012-arrangement-explain.sh \
  scripts/test-mu012-arrangement-package.sh \
  scripts/vet-mu012-arrangement-explain.sh
