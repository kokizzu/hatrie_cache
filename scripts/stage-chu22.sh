#!/bin/sh
set -eu

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CHU22_DIRECT_COLUMNAR_APPEND.md \
  INSPIRATION.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/chu22_columnar_append_benchmark_test.go \
  hat/hatSql/chu22_columnar_append_test.go \
  hat/hatSql/typed_table_columnar_append.go \
  scripts/benchmark-chu22.sh \
  scripts/commit-chu22.sh \
  scripts/format-chu22.sh \
  scripts/push-chu22.sh \
  scripts/race-chu22.sh \
  scripts/review-chu22.sh \
  scripts/stage-chu22.sh \
  scripts/test-chu22-all.sh \
  scripts/test-chu22-package.sh \
  scripts/test-chu22.sh \
  scripts/vet-chu22.sh

git diff --cached --check
git diff --cached --stat
git diff --cached --name-only
