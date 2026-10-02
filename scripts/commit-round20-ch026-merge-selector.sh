#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
if git diff --cached --quiet; then
  printf '%s\n' 'no staged CH-026 changes'
  exit 1
fi
git commit -m 'feat(storage): add deterministic compaction merge selectors [skip ci]'
