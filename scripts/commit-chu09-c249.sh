#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf '%s\n' 'no staged CH-U09 changes; run make stage-chu09-c249 first' >&2
  exit 1
fi
git diff --cached --check
git commit -m 'feat: persist versioned SQL result cache [skip ci]'
printf 'committed: %s\n' "$(git rev-parse --short HEAD)"
