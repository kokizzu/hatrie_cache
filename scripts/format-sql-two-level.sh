#!/bin/sh
set -eu

gofmt -w \
  hat/hatSql/columnar_vector_group_aggregate.go \
  hat/hatSql/hash_group_aggregate.go \
  hat/hatSql/two_level_group_aggregate_test.go \
  hat/hatSql/two_level_group_aggregate_benchmark_test.go

for file in \
  SQL_TWO_LEVEL_AGGREGATION.md \
  scripts/verify-inspiration.sh \
  scripts/test-sql-two-level.sh \
  scripts/benchmark-sql-two-level.sh \
  scripts/benchmark-sql-two-level-before.sh \
  scripts/format-sql-two-level.sh \
  scripts/test-race-sql-two-level.sh \
  scripts/vet-sql-two-level.sh \
  scripts/commit-sql-two-level.sh
do
  sed -i '${/^$/d;}' "$file"
done
