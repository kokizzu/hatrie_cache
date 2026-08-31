#!/bin/sh
set -eu

paths='
ADOPTED_QUERY_ENGINE_IDEAS.md
Makefile
hat/hatCache/sql_columnar_layout_cache.go
hat/hatCache/sql_columnar_sorted_projection_benchmark_test.go
hat/hatCache/sql_columnar_sorted_projection_test.go
hat/hatCache/sql_query.go
hat/hatSql/contracts.go
hat/hatSql/query.go
scripts/benchmark-sql-columnar-directed-composite-projection.sh
scripts/deliver-sql-columnar-directed-composite-projection.sh
scripts/test-sql-columnar-sorted-projection.sh
'

git diff --check -- $paths
git add -- $paths
git diff --cached --quiet -- $paths && {
	echo "no directed composite columnar projection changes staged" >&2
	exit 1
}
git commit -m "add directed composite columnar projections" -- $paths
git push
