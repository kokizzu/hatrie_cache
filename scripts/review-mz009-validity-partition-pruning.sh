#!/usr/bin/env bash
set -euo pipefail

feature_files=(
  Makefile
  README.md
  BENCHMARK.md
  ENGINE_IDEAS.md
  SQL_TEMPORAL_VALIDITY.md
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
  MZ009_VALIDITY_PARTITION_PRUNING.md
  hat/hatSql/contracts.go
  hat/hatSql/query.go
  hat/hatSql/mz009_validity_partition_pruning_test.go
  scripts/test-mz009-validity-partition-pruning.sh
  scripts/benchmark-mz009-validity-partition-pruning.sh
  scripts/format-mz009-validity-partition-pruning.sh
  scripts/test-mz009-validity-partition-pruning-package.sh
  scripts/race-mz009-validity-partition-pruning.sh
  scripts/vet-mz009-validity-partition-pruning.sh
  scripts/verify-mz009-validity-partition-pruning-docs.sh
  scripts/review-mz009-validity-partition-pruning.sh
  scripts/stage-mz009-validity-partition-pruning.sh
  scripts/commit-mz009-validity-partition-pruning.sh
  scripts/push-mz009-validity-partition-pruning.sh
)

for file in "${feature_files[@]}"; do
  test -e "$file"
done

git diff --check -- "${feature_files[@]}"
test "$(grep -c '^# MZ009_VALIDITY_PARTITION_PRUNING_FEATURE_TARGETS_BEGIN$' Makefile)" = 1
test "$(grep -c '^# MZ009_VALIDITY_PARTITION_PRUNING_FEATURE_TARGETS_END$' Makefile)" = 1
test ! -e scripts/inspect-sql-resolver-api-codex.sh
test ! -e scripts/show-mz009-test-codex.sh
test ! -e scripts/show-mz009-doc-context-codex.sh
test ! -e scripts/show-sql-temporal-codex.sh
test ! -e scripts/show-mz009-target-block-codex.sh
