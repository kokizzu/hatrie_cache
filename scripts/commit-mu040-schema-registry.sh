#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
if git diff --cached --quiet; then
  printf '%s\n' 'No staged M-U40 changes to commit.' >&2
  exit 1
fi
git commit -m 'feat(sql): add source schema registry'
