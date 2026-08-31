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
scripts/benchmark-sql-columnar-sorted-projection.sh
scripts/deliver-sql-columnar-composite-projection.sh
scripts/test-sql-columnar-sorted-projection.sh
'

git diff --check -- $paths
git add -- $paths
git diff --cached --quiet -- $paths && {
	echo "no composite columnar projection changes staged" >&2
	exit 1
}
git commit -m "add composite columnar sorted projections" -- $paths
git push
