#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf 'refusing to commit: no staged numeric IN changes\n' >&2
  exit 1
fi
git diff --cached --check
git commit -m 'feat(sql): accelerate numeric in predicates'
