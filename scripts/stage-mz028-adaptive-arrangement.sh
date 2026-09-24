#!/usr/bin/env bash
set -euo pipefail

git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md ENGINE_IDEAS.md MZ028_ADAPTIVE_ARRANGEMENT.md Makefile hat/hatSql/typed_table.go hat/hatSql/typed_table_aggregate_dictionary.go hat/hatSql/typed_table_aggregate_merge.go hat/hatSql/mz028_adaptive_arrangement_test.go scripts/mz028-adaptive-arrangement.sh scripts/verify-ledger-corrections.sh scripts/verify-mz028-adaptive-arrangement.sh scripts/stage-mz028-adaptive-arrangement.sh scripts/commit-mz028-adaptive-arrangement.sh scripts/push-mz028-adaptive-arrangement.sh
git diff --cached --check
git status --short
