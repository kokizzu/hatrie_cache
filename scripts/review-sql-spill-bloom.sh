#!/bin/sh
set -eu

git diff --check
git status --short
git diff -- Makefile scripts/audit-query-performance-goal.sh scripts/test-sql-spill-bloom.sh scripts/format-sql-spill-bloom.sh hat/hatSql/query.go hat/hatSql/spill_bloom_test.go
