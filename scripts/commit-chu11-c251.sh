#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf '%s\n' 'no staged CH-U11 changes to commit' >&2
  exit 1
fi
git diff --cached --check
git commit -m 'feat: add workload-driven SQL skip-index selection [skip ci]'
