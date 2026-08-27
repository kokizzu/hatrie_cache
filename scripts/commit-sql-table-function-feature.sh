#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
git diff --check
git add Makefile SQL.md hat/hatSql/query.go hat/hatSql/table_function.go hat/hatSql/table_function_test.go scripts/test-sql-table-functions.sh scripts/format-sql-table-functions.sh scripts/verify-sql-table-function-feature.sh scripts/inspect-sql-table-function-feature.sh scripts/show-sql-source-model.sh scripts/show-sql-source-execution.sh scripts/commit-sql-table-function-feature.sh
git commit -m "feat: add SQL table functions"
git push
