#!/bin/sh
set -eu

git add Makefile scripts/audit-query-performance-goal.sh scripts/test-sql-spill-compression.sh scripts/format-sql-spill-compression.sh scripts/review-sql-spill-compression.sh scripts/commit-sql-spill-compression.sh hat/hatSql/query.go hat/hatSql/spill_compression_test.go
git commit -m 'feat: add SQL spill compression'
git push
