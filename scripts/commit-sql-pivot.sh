#!/bin/sh
set -eu

git add \
  Makefile \
  scripts/test-sql-pivot.sh \
  scripts/format-sql-pivot.sh \
  scripts/review-sql-pivot.sh \
  scripts/commit-sql-pivot.sh \
  hat/hatSql/pivot.go \
  hat/hatSql/pivot_test.go
git diff --cached --check
git commit -m 'feat: add SQL pivot helpers'
git push
