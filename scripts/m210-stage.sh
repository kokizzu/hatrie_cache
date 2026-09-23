#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M210_RETAINED_STATE.md \
  Makefile \
  README.md \
  hat/hatSql/m210_retained_state_benchmark_test.go \
  hat/hatSql/m210_retained_state_test.go \
  hat/hatSql/retained_state.go \
  scripts/m210-benchmark.sh \
  scripts/m210-commit.sh \
  scripts/m210-format.sh \
  scripts/m210-push.sh \
  scripts/m210-race.sh \
  scripts/m210-stage.sh \
  scripts/m210-test.sh \
  scripts/m210-vet.sh

git diff --cached --check
git status --short
git diff --cached --stat
