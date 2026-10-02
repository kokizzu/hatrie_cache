#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  BENCHMARK.md \
  CHU47_SQL_DICTIONARY_FUNCTIONS.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/dictionary.go \
  hat/hatSql/sql_dictionary_test.go \
  scripts/benchmark-chu47.sh \
  scripts/format-chu47.sh \
  scripts/race-chu47.sh \
  scripts/test-chu47-full.sh \
  scripts/test-chu47-package.sh \
  scripts/test-chu47.sh \
  scripts/verify-chu47-docs.sh \
  scripts/vet-chu47.sh \
  scripts/status-chu47.sh \
  scripts/stage-chu47.sh \
  scripts/commit-chu47.sh \
  scripts/push-chu47.sh

git diff --cached --check
git diff --cached --stat
