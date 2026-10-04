#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add -- \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU019_TUPLE_UPDATE_JOURNAL.md \
  hat/hatDataStructure/tuple_update_journal.go \
  hat/hatDataStructure/tu19_tuple_update_journal_benchmark_test.go \
  hat/hatDataStructure/tu19_tuple_update_journal_compare_test.go \
  hat/hatDataStructure/tu19_tuple_update_journal_test.go \
  scripts/benchmark-tu19.sh \
  scripts/commit-tu19.sh \
  scripts/format-tu19.sh \
  scripts/push-tu19.sh \
  scripts/stage-tu19.sh \
  scripts/test-all-tu19.sh \
  scripts/test-tu19-package.sh \
  scripts/test-tu19.sh \
  scripts/verify-tu19.sh
git status --short
