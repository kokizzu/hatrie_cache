#!/bin/sh
set -eu

git diff --check
git diff -- ADOPTED_QUERY_ENGINE_IDEAS.md Makefile hat/hatCache/main.go hat/hatCache/sql_query.go hat/hatCache/sql_partial_index.go hat/hatCache/sql_partial_index_benchmark_test.go hat/hatCache/sql_partial_index_test.go scripts/test-sql-partial-index.sh scripts/benchmark-sql-partial-index.sh scripts/verify-sql-partial-index.sh scripts/deliver-sql-partial-index-plan.sh scripts/deliver-sql-partial-index.sh
