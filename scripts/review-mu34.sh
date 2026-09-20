#!/usr/bin/env bash
set -euo pipefail

if [[ -e hat/hatSql/mu34_compile_shim.go ]]; then
  printf '%s\n' 'temporary M-U34 SQL compile shim must be removed' >&2
  exit 1
fi

git diff --check
git status --short
