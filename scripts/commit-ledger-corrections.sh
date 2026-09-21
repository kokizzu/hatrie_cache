#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf '%s\n' 'no staged ledger correction changes' >&2
  exit 1
fi

git commit -m 'docs: correct adopted idea ledger'
