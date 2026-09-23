#!/usr/bin/env bash
set -euo pipefail

make verify-t220-scope

if git diff --cached --quiet; then
  printf 'No staged T220 changes. Run make stage-t220 first.\n' >&2
  exit 1
fi

git commit -m 'docs: document r-tree index'
