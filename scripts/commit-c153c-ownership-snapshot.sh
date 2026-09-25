#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"
bash scripts/stage-c153c-ownership-snapshot.sh
git diff --cached --check
git commit -m "feat: add durable ownership snapshots"
