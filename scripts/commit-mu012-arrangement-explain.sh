#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  echo 'no staged M-U12 changes' >&2
  exit 1
fi
git commit -m 'feat: expose arrangement metadata in EXPLAIN [skip ci]'
