#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/stage-t111.sh
git diff --cached --check
if git diff --cached --quiet; then
  printf '%s\n' 'no staged T111 changes to commit' >&2
  exit 1
fi
git commit -m 'feat: separate cache and durable storage sizing'
