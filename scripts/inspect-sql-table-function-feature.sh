#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
git status --short
git diff --check
git diff -- Makefile SQL.md hat/hatSql/query.go hat/hatSql/table_function.go hat/hatSql/table_function_test.go scripts/test-sql-table-functions.sh scripts/format-sql-table-functions.sh scripts/verify-sql-table-function-feature.sh scripts/show-sql-source-model.sh scripts/show-sql-source-execution.sh scripts/inspect-sql-table-function-feature.sh
