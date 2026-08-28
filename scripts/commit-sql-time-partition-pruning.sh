#!/bin/sh
set -eu

git add ./Makefile ./scripts/audit-query-performance-goal.sh ./scripts/test-sql-time-partition-pruning.sh ./scripts/format-sql-time-partition-pruning.sh ./scripts/review-sql-time-partition-pruning.sh ./scripts/commit-sql-time-partition-pruning.sh ./hat/hatCache/main.go ./hat/hatCache/sql_query.go ./hat/hatCache/sql_time_partition_pruning_test.go
git commit -m 'feat: prune configured SQL time partitions'
git push
