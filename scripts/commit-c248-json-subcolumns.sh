#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  echo "No staged C248 changes to commit." >&2
  exit 1
fi
git commit -m "docs(inspiration): catalog variant JSON subcolumns"
