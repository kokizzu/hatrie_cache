#!/bin/sh
set -eu

git add \
  Makefile \
  SQL.md \
  SQL_TEST_MATRIX.md \
  scripts/audit-sql-improvements.sh \
  scripts/verify-sql-improvement-docs.sh \
  scripts/review-sql-improvement-docs.sh \
  scripts/commit-sql-improvement-docs.sh
git diff --cached --check
git commit -m 'docs: cover advanced SQL improvements'
git push
