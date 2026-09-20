#!/usr/bin/env bash
set -euo pipefail

branch=$(git branch --show-current)
if [[ "$branch" != 'codex/tt051-partition-sync-durability' ]]; then
  printf 'unexpected branch: %s\n' "$branch" >&2
  exit 1
fi
git push -u origin "$branch"
