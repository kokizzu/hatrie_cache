#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf '%s\n' 'no staged CH-012 changes to commit' >&2
  exit 1
fi
git diff --cached --check
git commit -m 'optimize logical delete bitmap storage'
