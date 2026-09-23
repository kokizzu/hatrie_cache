#!/usr/bin/env bash
set -euo pipefail

make verify-t221-scope

if git diff --cached --quiet; then
  printf 'No staged T221 changes. Run make stage-t221 first.\n' >&2
  exit 1
fi

git commit -m 'docs: document bitmap index'
