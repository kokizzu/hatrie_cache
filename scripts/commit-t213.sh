#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  echo 'No staged T213 changes to commit.'
  exit 1
fi

git commit -m 'snapshots: add scheduled checkpoint publication'
