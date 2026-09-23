#!/usr/bin/env bash
set -euo pipefail

make verify-t218-scope

if git diff --cached --quiet; then
  printf 'No staged T218 changes. Run make stage-t218 first.\n' >&2
  exit 1
fi

git commit -m 'feat: add multi-part tree index'
