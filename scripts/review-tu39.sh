#!/usr/bin/env bash
set -euo pipefail

if [[ -e hat/hatSql/tu39_compile_shim.go ]]; then
  printf '%s\n' 'temporary TU39 SQL compile shim must be removed' >&2
  exit 1
fi

git diff --check
git status --short
