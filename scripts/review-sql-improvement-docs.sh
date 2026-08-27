#!/bin/sh
set -eu

git diff --check -- \
  Makefile \
  SQL.md \
  SQL_TEST_MATRIX.md \
  scripts/audit-sql-improvements.sh \
  scripts/verify-sql-improvement-docs.sh \
  scripts/review-sql-improvement-docs.sh \
  scripts/commit-sql-improvement-docs.sh
git status --short -- \
  Makefile \
  SQL.md \
  SQL_TEST_MATRIX.md \
  scripts/audit-sql-improvements.sh \
  scripts/verify-sql-improvement-docs.sh \
  scripts/review-sql-improvement-docs.sh \
  scripts/commit-sql-improvement-docs.sh
