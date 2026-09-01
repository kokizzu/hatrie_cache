#!/bin/sh
set -eu

git diff --check
git diff -- ADOPTED_QUERY_ENGINE_IDEAS.md Makefile TYPED_TABLES.md hat/hatSql/typed_table.go hat/hatSql/typed_table_arrangements.go hat/hatSql/typed_table_distinct_benchmark_test.go hat/hatSql/typed_table_distinct_test.go scripts/test-sql-typed-distinct.sh scripts/benchmark-sql-typed-distinct.sh scripts/verify-sql-typed-distinct.sh scripts/deliver-sql-typed-distinct-plan.sh scripts/deliver-sql-typed-distinct.sh
