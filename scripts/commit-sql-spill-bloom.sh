#!/bin/sh
set -eu

git add Makefile scripts/audit-query-performance-goal.sh scripts/test-sql-spill-bloom.sh scripts/format-sql-spill-bloom.sh scripts/review-sql-spill-bloom.sh scripts/commit-sql-spill-bloom.sh hat/hatSql/query.go hat/hatSql/spill_bloom_test.go
git commit -m 'feat: filter SQL spill hash partitions'
git push
