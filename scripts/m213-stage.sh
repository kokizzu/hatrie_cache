#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M213_DIFFERENTIAL_CONSOLIDATION.md \
  Makefile \
  README.md \
  hat/hatSql/debezium_changefeed.go \
  hat/hatSql/differential_subscription.go \
  hat/hatSql/m213_differential_consolidation_benchmark_test.go \
  hat/hatSql/m213_differential_consolidation_test.go \
  scripts/m213-benchmark.sh \
  scripts/m213-commit.sh \
  scripts/m213-format.sh \
  scripts/m213-push.sh \
  scripts/m213-race.sh \
  scripts/m213-stage.sh \
  scripts/m213-test.sh \
  scripts/m213-vet.sh
git diff --cached --check
git status --short
git diff --cached --stat
