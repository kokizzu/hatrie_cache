#!/usr/bin/env bash
set -euo pipefail

make verify-t219-scope

if git diff --cached --quiet; then
  printf 'No staged T219 changes. Run make stage-t219 first.\n' >&2
  exit 1
fi

git commit -m 'docs: document hash index'
