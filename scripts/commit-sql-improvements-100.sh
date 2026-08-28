#!/usr/bin/env sh
set -eu

make review-sql-improvements-100
git add -- Makefile SQL_IMPROVEMENTS_100.md scripts/audit-sql-improvements-100.sh scripts/verify-sql-improvements-100.sh scripts/review-sql-improvements-100.sh scripts/commit-sql-improvements-100.sh
git diff --cached --check
git commit --only -m "docs: add SQL improvement backlog" -- Makefile SQL_IMPROVEMENTS_100.md scripts/audit-sql-improvements-100.sh scripts/verify-sql-improvements-100.sh scripts/review-sql-improvements-100.sh scripts/commit-sql-improvements-100.sh
git push
