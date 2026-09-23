#!/usr/bin/env bash
set -euo pipefail

branch=$(git branch --show-current)
if [[ -z "$branch" ]]; then
  printf 'cannot push without a current branch\n' >&2
  exit 1
fi
git push --set-upstream origin "$branch"
