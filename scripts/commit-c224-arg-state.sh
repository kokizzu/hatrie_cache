#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf '%s\n' 'No staged C224 changes to commit.' >&2
  exit 1
fi

git commit -m 'feat: add importable arg extreme states'
