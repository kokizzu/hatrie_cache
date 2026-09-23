#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/verify-t225-scope.sh
git diff --cached --check
if git diff --cached --quiet; then
  printf '%s\n' 'No staged T225 changes.' >&2
  exit 1
fi
git commit -m 'docs: document covering indexes'
