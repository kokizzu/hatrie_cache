#!/bin/sh
set -eu

git status --short
git diff --check
git diff -- ADOPTED_QUERY_ENGINE_IDEAS.md hat/hatSql/typed_table.go hat/hatSql/typed_table_arrangements.go hat/hatSql/typed_table_benchmark_test.go hat/hatSql/typed_table_minmax_test.go scripts/test-sql-typed-minmax.sh scripts/benchmark-sql-typed-minmax.sh scripts/verify-sql-typed-minmax.sh scripts/deliver-sql-typed-minmax-plan.sh scripts/deliver-sql-typed-minmax.sh
git diff -- Makefile
