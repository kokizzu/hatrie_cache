#!/bin/sh
set -eu

paths='Makefile README.md TYPED_TABLE_ARRANGEMENTS.md hat/hatSql/typed_table_arrangements.go hat/hatSql/typed_table_arrangements_test.go hat/hatSql/typed_table_arrangements_benchmark_test.go scripts/test-sql-typed-arrangements.sh scripts/benchmark-sql-typed-arrangements.sh scripts/format-sql-typed-arrangements.sh scripts/verify-sql-typed-arrangements.sh scripts/test-race-sql-typed-arrangements.sh scripts/security-sql-typed-arrangements.sh scripts/inspect-sql-typed-arrangements.sh scripts/deliver-sql-typed-arrangements.sh'

case "${1:-}" in
commit)
	git diff --check -- $paths
	git add -- $paths
	git commit -m 'share typed table aggregate arrangements' -- $paths
	;;
push)
	git push
	;;
*)
	echo "usage: $0 {commit|push}" >&2
	exit 2
	;;
esac
