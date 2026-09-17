#!/bin/sh
set -eu

if git diff --cached --quiet; then
  printf '%s\n' 'no staged MZ-031 changes to commit' >&2
  exit 1
fi
git diff --cached --check
git commit -m 'Add incremental Top-K rank diffs [skip ci]'
