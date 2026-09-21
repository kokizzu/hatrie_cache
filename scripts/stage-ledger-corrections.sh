#!/usr/bin/env bash
set -euo pipefail

git add ENGINE_IDEAS.md Makefile scripts/verify-ledger-corrections.sh scripts/stage-ledger-corrections.sh scripts/commit-ledger-corrections.sh scripts/push-ledger-corrections.sh
git diff --cached --check
git status --short
