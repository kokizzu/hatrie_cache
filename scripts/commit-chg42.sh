#!/usr/bin/env bash
set -euo pipefail

git add \
  CHG42_OPERATOR_MEMORY.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  README.md \
  hat/hatSql/operator_memory.go \
  hat/hatSql/operator_memory_test.go \
  hat/hatSql/chg42_operator_memory_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-chg42.sh \
  scripts/commit-chg42.sh \
  scripts/format-chg42-tracker.sh \
  scripts/push-chg42.sh \
  scripts/race-chg42.sh \
  scripts/test-chg42-full.sh \
  scripts/test-chg42-package.sh \
  scripts/test-chg42-tracker.sh \
  scripts/vet-chg42.sh \
  Makefile
git commit -m 'feat(sql): add opt-in operator memory tracking'
