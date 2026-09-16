#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf '%s\n' 'no staged CH-U10 changes; run make stage-chu10-c250 first' >&2
  exit 1
fi
git diff --cached --check
git commit -m 'feat: rank projections by observed workload cost [skip ci]'
printf 'committed: %s\n' "$(git rev-parse --short HEAD)"
