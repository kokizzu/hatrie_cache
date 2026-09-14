#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf '%s\n' 'no staged CH-028 changes' >&2
  exit 1
fi
git commit -m 'feat: add dictionary version and fallback semantics'
