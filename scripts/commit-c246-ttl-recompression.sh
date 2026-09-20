#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  echo "No staged C246 changes to commit." >&2
  exit 1
fi
git commit -m "feat(data): add TTL recompression policy"
