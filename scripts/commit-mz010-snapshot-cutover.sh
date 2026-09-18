#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf '%s\n' 'No staged MZ010 snapshot cutover changes.' >&2
  exit 1
fi
git commit -m 'Add cross-source snapshot cutover coordinator [skip ci]'
