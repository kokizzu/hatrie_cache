#!/usr/bin/env bash
set -euo pipefail

git add -- \
  INSPIRATION_ROUND2.md \
  Makefile \
  scripts/stage-c221-ledger.sh \
  scripts/commit-c221-ledger.sh \
  scripts/push-c221-ledger.sh
git diff --cached --check
git diff --cached --stat
git status --short
