#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M216_INCREMENTAL_TOP_K.md \
  Makefile \
  README.md \
  scripts/m216-benchmark.sh \
  scripts/m216-commit.sh \
  scripts/m216-push.sh \
  scripts/m216-stage.sh \
  scripts/m216-test.sh
git diff --cached --check
git diff --cached --stat
