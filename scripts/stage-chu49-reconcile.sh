#!/usr/bin/env bash
set -euo pipefail

git add \
  CHU49_SKIP_INDEX_EXPLAIN.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  scripts/commit-chu49-reconcile.sh \
  scripts/push-chu49-reconcile.sh \
  scripts/stage-chu49-reconcile.sh \
  scripts/verify-chu49-focused.sh

git diff --cached --check
git diff --cached --stat
git status --short
