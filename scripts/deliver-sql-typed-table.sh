#!/bin/sh
set -eu

paths='TYPED_TABLES.md hat/hatSql/typed_table.go hat/hatSql/typed_table_benchmark_test.go hat/hatSql/typed_table_test.go scripts/benchmark-sql-typed-table.sh scripts/deliver-sql-typed-table.sh'

if ! git diff --cached --quiet; then
	echo 'refusing to deliver with pre-existing staged changes' >&2
	exit 1
fi

git diff --check -- $paths
git add -- $paths
git diff --cached --check
git commit -m 'feat: cache typed table columnar layouts'
git push
