#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
if git diff --cached --quiet; then
  printf 'no staged MZ-028 changes\n' >&2
  exit 1
fi
git commit -m 'perf(storage): skip redundant spill compaction [skip ci]'
