#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf 'No staged T-U05 changes to commit.\n' >&2
  exit 1
fi
git commit -m 'feat(sql): add session transaction settings'
