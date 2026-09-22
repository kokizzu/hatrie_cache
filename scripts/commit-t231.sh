#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf '%s\n' 'No staged T231 changes.' >&2
  exit 1
fi
git commit -m "feat: add memtx after-replace audit hooks"
