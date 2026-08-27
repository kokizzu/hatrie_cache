#!/bin/sh
set -eu

git diff --check -- \
  Makefile \
  scripts/test-sql-pivot.sh \
  scripts/format-sql-pivot.sh \
  scripts/review-sql-pivot.sh \
  scripts/commit-sql-pivot.sh \
  hat/hatSql/pivot.go \
  hat/hatSql/pivot_test.go
git status --short -- \
  Makefile \
  scripts/test-sql-pivot.sh \
  scripts/format-sql-pivot.sh \
  scripts/review-sql-pivot.sh \
  scripts/commit-sql-pivot.sh \
  hat/hatSql/pivot.go \
  hat/hatSql/pivot_test.go
