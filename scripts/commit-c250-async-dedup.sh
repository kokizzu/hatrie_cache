#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  echo "No staged C250 changes to commit." >&2
  exit 1
fi
git commit -m "docs(inspiration): catalog retry-safe async insert identities"
