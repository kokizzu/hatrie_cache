#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat -- ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md ENGINE_IDEAS.md Makefile README.md SQL_PARTITION_RANGE_PRUNING.md hat/hatCache/sql_query.go hat/hatSql/contracts.go hat/hatSql/partitioned_source_test.go hat/hatSql/query.go scripts/benchmark-sql-range-partition-pruning.sh scripts/format-sql-range-partition-pruning.sh scripts/test-race-sql-range-partition-pruning.sh scripts/test-sql-range-partition-pruning.sh scripts/verify-sql-range-partition-pruning-docs.sh scripts/vet-sql-range-partition-pruning.sh
git diff --unified=3 -- ENGINE_IDEAS.md hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/partitioned_source_test.go hat/hatCache/sql_query.go
