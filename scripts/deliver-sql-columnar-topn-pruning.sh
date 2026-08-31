#!/bin/sh
set -eu

paths='
ADOPTED_QUERY_ENGINE_IDEAS.md
Makefile
hat/hatCache/sql_columnar_topn_segment_pruning_test.go
hat/hatSql/columnar_topn_benchmark_test.go
hat/hatSql/columnar_topn_test.go
hat/hatSql/query.go
scripts/benchmark-sql-columnar-topn-pruning.sh
scripts/benchmark-sql-columnar-topn.sh
scripts/deliver-sql-columnar-topn-pruning.sh
scripts/format-sql-columnar-topn.sh
scripts/test-sql-columnar-topn.sh
'

git diff --check -- $paths
git add -- $paths
git diff --cached --quiet -- $paths && {
	echo "no columnar Top-N pruning changes staged" >&2
	exit 1
}
git commit -m "add numeric segment top-n pruning" -- $paths
git push
