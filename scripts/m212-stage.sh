#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M212_LOGICAL_COMPACTION.md \
  Makefile \
  README.md \
  hat/hatSql/m212_retained_state_benchmark_test.go \
  hat/hatSql/m212_retained_state_test.go \
  hat/hatSql/retained_state.go \
  scripts/m212-benchmark.sh \
  scripts/m212-commit.sh \
  scripts/m212-format.sh \
  scripts/m212-push.sh \
  scripts/m212-race.sh \
  scripts/m212-stage.sh \
  scripts/m212-test.sh \
  scripts/m212-vet.sh
git diff --cached --check
git status --short
git diff --cached --stat
