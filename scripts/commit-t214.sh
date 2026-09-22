#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  echo 'No staged T214 changes to commit.' >&2
  exit 1
fi
git commit -m 'replication: stream snapshots without shared filesystem'
