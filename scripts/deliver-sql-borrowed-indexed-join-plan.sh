#!/bin/sh
set -eu

git status --short
git diff --check
git diff -- ADOPTED_QUERY_ENGINE_IDEAS.md hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/indexed_join_borrowed_test.go hat/hatCache/sql_borrowed_index.go hat/hatCache/sql_borrowed_index_test.go hat/hatCache/sql_borrowed_index_benchmark_test.go scripts/test-sql-borrowed-indexed-join.sh scripts/benchmark-sql-borrowed-indexed-join.sh scripts/verify-sql-borrowed-indexed-join.sh scripts/deliver-sql-borrowed-indexed-join-plan.sh scripts/deliver-sql-borrowed-indexed-join.sh
git diff -- Makefile
