#!/bin/sh
set -eu

git diff --cached --check
if git diff --cached --quiet; then
  echo "no staged changes"
  exit 1
fi
git commit -m 'feat: add opt-in volatile cache engine [skip ci]'
