#!/bin/sh
set -eu

paths='Makefile README.md TYPED_TABLES.md hat/hatSql/typed_table.go hat/hatSql/typed_table_benchmark_test.go hat/hatSql/typed_table_test.go scripts/audit-sql-typed-table-security.sh scripts/benchmark-sql-typed-table.sh scripts/deliver-sql-typed-table.sh scripts/format-sql-typed-table.sh scripts/inspect-readme-sql-docs.sh scripts/inspect-sql-analytics-primitives.sh scripts/inspect-sql-typed-table-delivery.sh scripts/test-race-sql-typed-table.sh scripts/test-sql-typed-table.sh scripts/verify-sql-typed-table-docs.sh'

if ! git diff --cached --quiet; then
	echo 'refusing to deliver with pre-existing staged changes' >&2
	exit 1
fi

git diff --check -- $paths
git add -- $paths
git diff --cached --check
git commit -m 'feat: add typed SQL table changefeed'
git push
