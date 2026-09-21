#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
if git diff --cached --quiet; then
  printf '%s\n' 'no staged changes to commit' >&2
  exit 1
fi
git commit -m "hatSql: index mutation dependency readiness"
