#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  echo 'No staged T215 changes to commit.' >&2
  exit 1
fi
git commit -m 'storage: add per-space memtx and on-disk policies'
