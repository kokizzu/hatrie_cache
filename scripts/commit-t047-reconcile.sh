#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"
bash scripts/stage-t047-reconcile.sh
git diff --cached --check
git commit -m "feat: add atomic participant reconciliation"
