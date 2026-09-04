#!/bin/sh
set -eu

printf '%s\n' '--- worktree status ---'
git status --short
printf '%s\n' '--- feature diff check ---'
git diff --check -- \
  hat/hatSql/hash_group_aggregate.go \
  hat/hatSql/hash_group_aggregate_test.go \
  hat/hatSql/query.go \
  SQL_HASH_AGGREGATE.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  scripts/benchmark-sql-hash-aggregate.sh \
  scripts/format-sql-hash-aggregate.sh \
  scripts/test-sql-hash-aggregate.sh
printf '%s\n' '--- feature diff stat ---'
git diff --stat -- \
  hat/hatSql/hash_group_aggregate.go \
  hat/hatSql/hash_group_aggregate_test.go \
  hat/hatSql/query.go \
  SQL_HASH_AGGREGATE.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  scripts/benchmark-sql-hash-aggregate.sh \
  scripts/format-sql-hash-aggregate.sh \
  scripts/test-sql-hash-aggregate.sh
printf '%s\n' '--- hash aggregate Makefile targets ---'
rg -n -C 3 'hash-aggregate' Makefile
