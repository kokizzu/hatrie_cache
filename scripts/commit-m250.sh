#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf '%s\n' 'No staged M250 changes.' >&2
  exit 1
fi
git commit -m 'feat: align temporal join frontiers'
