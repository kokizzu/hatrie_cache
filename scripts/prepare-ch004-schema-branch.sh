#!/usr/bin/env bash
set -euo pipefail

branch='codex/ch004-schema-metadata'
current=$(git branch --show-current)
if [[ -n "$current" ]]; then
    printf 'On branch %s\n' "$current"
    exit 0
fi

if git show-ref --verify --quiet "refs/heads/$branch"; then
    git switch "$branch"
else
    git switch -c "$branch"
fi
