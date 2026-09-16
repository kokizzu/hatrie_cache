#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf '%s\n' 'no staged MZ-020 changes' >&2
  exit 1
fi
git commit -m '[skip ci] Add resizable scheduler'
