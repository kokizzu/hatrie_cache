#!/usr/bin/env bash
set -euo pipefail
if git diff --cached --quiet; then
  printf '%s\n' 'No staged CH-U12 changes to commit.'
  exit 1
fi
git diff --cached --check
git commit -m 'adopt prioritized background index rebuild queue [skip ci]'
