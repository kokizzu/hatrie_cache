#!/usr/bin/env bash
set -euo pipefail
branch=$(git branch --show-current)
if [[ -z "$branch" ]]; then
  printf '%s\n' 'cannot determine current branch' >&2
  exit 1
fi
git push origin "HEAD:$branch"
