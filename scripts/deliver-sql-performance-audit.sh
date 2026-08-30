#!/usr/bin/env sh
set -eu

git add -- \
  Makefile \
  scripts/deliver-sql-performance-audit.sh \
  scripts/inspect-next-sql-performance-opportunities.sh
git diff --cached --check
git commit -m "chore(sql): add performance opportunity audit"
git push
