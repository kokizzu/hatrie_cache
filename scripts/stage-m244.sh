#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M244_COMPACTION_DEBT.md \
  README.md \
  Makefile \
  hat/hatSql/m244_compaction_debt_benchmark_test.go \
  hat/hatSql/m244_compaction_debt_test.go \
  hat/hatSql/typed_table_arrangement_stats.go \
  scripts/benchmark-m244.sh \
  scripts/commit-m244.sh \
  scripts/format-m244.sh \
  scripts/push-m244.sh \
  scripts/race-m244.sh \
  scripts/stage-m244.sh \
  scripts/test-m244-package.sh \
  scripts/test-m244.sh \
  scripts/verify-docs-m244.sh \
  scripts/vet-m244.sh

git diff --cached --check
git status --short
