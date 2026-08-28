#!/bin/sh
set -eu

git diff --check
git diff --stat -- ./Makefile ./scripts/audit-query-performance-goal.sh ./scripts/test-sql-time-partition-pruning.sh ./scripts/format-sql-time-partition-pruning.sh ./hat/hatCache/main.go ./hat/hatCache/sql_query.go ./hat/hatCache/sql_time_partition_pruning_test.go
git status --short
