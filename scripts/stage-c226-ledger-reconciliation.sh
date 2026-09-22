#!/usr/bin/env bash
set -euo pipefail

git add -- \
  INSPIRATION_ROUND2.md \
  Makefile \
  scripts/stage-c226-ledger-reconciliation.sh \
  scripts/commit-c226-ledger-reconciliation.sh \
  scripts/push-c226-ledger-reconciliation.sh
git diff --cached --check
git diff --cached --stat
git status --short
