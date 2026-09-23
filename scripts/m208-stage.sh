#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M208_DIFFERENTIAL_MULTIPLICITY.md \
  Makefile \
  README.md \
  hat/hatSql/debezium_changefeed.go \
  hat/hatSql/m208_differential_multiplicity_benchmark_test.go \
  hat/hatSql/m208_differential_multiplicity_test.go \
  hat/hatSql/query_subscription_multiplicity.go \
  scripts/m208-benchmark.sh \
  scripts/m208-commit.sh \
  scripts/m208-format.sh \
  scripts/m208-push.sh \
  scripts/m208-race.sh \
  scripts/m208-stage.sh \
  scripts/m208-test.sh \
  scripts/m208-vet.sh

git diff --cached --check
git status --short
git diff --cached --stat
