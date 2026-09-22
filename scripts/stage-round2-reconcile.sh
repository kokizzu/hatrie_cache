#!/usr/bin/env bash
set -euo pipefail

git add Makefile INSPIRATION_ROUND2.md scripts/stage-round2-reconcile.sh scripts/commit-round2-reconcile.sh scripts/push-round2-reconcile.sh
git diff --cached --check
git diff --cached --name-status
