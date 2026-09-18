#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf '%s\n' 'No staged temporary-directory cleanup changes.' >&2
  exit 1
fi
git commit -m 'Harden stale test directory cleanup [skip ci]'
