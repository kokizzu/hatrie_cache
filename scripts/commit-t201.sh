#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf '%s\n' 'No staged T201 changes.' >&2
  exit 1
fi
git commit -m 'feat: add per-space replication quorum'
