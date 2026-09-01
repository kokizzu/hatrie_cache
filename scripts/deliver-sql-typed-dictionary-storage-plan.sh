#!/bin/sh
set -eu

git diff --check
git diff -- ADOPTED_QUERY_ENGINE_IDEAS.md Makefile TYPED_TABLES.md hat/hatSql/typed_table.go hat/hatSql/typed_table_dictionary_storage_benchmark_test.go hat/hatSql/typed_table_dictionary_storage_test.go scripts/test-sql-typed-dictionary-storage.sh scripts/benchmark-sql-typed-dictionary-storage.sh scripts/verify-sql-typed-dictionary-storage.sh scripts/deliver-sql-typed-dictionary-storage-plan.sh scripts/deliver-sql-typed-dictionary-storage.sh
