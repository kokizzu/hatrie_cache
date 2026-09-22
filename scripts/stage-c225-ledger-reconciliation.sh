#!/usr/bin/env bash
set -euo pipefail

git add -- \
  INSPIRATION_ROUND2.md \
  Makefile \
  scripts/stage-c225-ledger-reconciliation.sh \
  scripts/commit-c225-ledger-reconciliation.sh \
  scripts/push-c225-ledger-reconciliation.sh
git diff --cached --check
git diff --cached --stat
git status --short
