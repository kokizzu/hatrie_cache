#!/bin/sh
set -eu

git diff --check
git status --short
git diff -- Makefile scripts/audit-query-performance-goal.sh scripts/test-sql-spill-compression.sh scripts/format-sql-spill-compression.sh hat/hatSql/query.go hat/hatSql/spill_compression_test.go
