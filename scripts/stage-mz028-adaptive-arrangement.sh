#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  ENGINE_IDEAS.md \
  MZ028_ADAPTIVE_ARRANGEMENT.md \
  Makefile \
  hat/hatSql/mz028_adaptive_arrangement_test.go \
  hat/hatSql/mz028_batched_merge_test.go \
  hat/hatSql/typed_table.go \
  hat/hatSql/typed_table_aggregate_dictionary.go \
  hat/hatSql/typed_table_aggregate_merge.go \
  scripts/benchmark-mz028-batched-merge.sh \
  scripts/commit-mz028-adaptive-arrangement.sh \
  scripts/format-mz028-adaptive-arrangement.sh \
  scripts/mz028-adaptive-arrangement.sh \
  scripts/push-mz028-adaptive-arrangement.sh \
  scripts/race-mz028-adaptive-arrangement.sh \
  scripts/review-mz028-adaptive-arrangement.sh \
  scripts/stage-mz028-adaptive-arrangement.sh \
  scripts/test-mz028-adaptive-arrangement.sh \
  scripts/test-mz028-batched-merge.sh \
  scripts/test-mz028-package.sh \
  scripts/verify-ledger-corrections.sh \
  scripts/verify-mz028-adaptive-arrangement.sh \
  scripts/vet-mz028-adaptive-arrangement.sh
git diff --cached --check
git status --short
