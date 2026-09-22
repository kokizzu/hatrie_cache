#!/usr/bin/env bash
set -euo pipefail

git add -- \
  INSPIRATION_ROUND2.md \
  Makefile \
  hat/hatSql/chu02_external_order_spill_test.go \
  scripts/stage-c228-ledger-reconciliation.sh \
  scripts/commit-c228-ledger-reconciliation.sh \
  scripts/push-c228-ledger-reconciliation.sh
git diff --cached --check
git diff --cached --stat
git status --short
