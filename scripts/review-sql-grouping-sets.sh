#!/bin/sh
set -eu

git diff --check -- \
  Makefile \
  scripts/report-package-layout.sh \
  scripts/test-sql-grouping-sets.sh \
  scripts/format-sql-grouping-sets.sh \
  scripts/review-sql-grouping-sets.sh \
  scripts/commit-sql-grouping-sets.sh \
  hat/hatSql/grouping_sets.go \
  hat/hatSql/grouping_sets_test.go \
  hat/hatSql/query.go
git status --short -- \
  Makefile \
  scripts/report-package-layout.sh \
  scripts/test-sql-grouping-sets.sh \
  scripts/format-sql-grouping-sets.sh \
  scripts/review-sql-grouping-sets.sh \
  scripts/commit-sql-grouping-sets.sh \
  hat/hatSql/grouping_sets.go \
  hat/hatSql/grouping_sets_test.go \
  hat/hatSql/query.go
