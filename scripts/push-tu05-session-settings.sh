#!/usr/bin/env bash
set -euo pipefail

branch=$(git branch --show-current)
if [[ "$branch" != 'codex/tu05-session-transaction-settings' ]]; then
  printf 'Refusing to push unexpected branch: %s\n' "$branch" >&2
  exit 1
fi
git push -u origin "$branch"
