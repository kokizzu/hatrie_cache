#!/usr/bin/env bash
set -eu

mode=${1:-test}
case "$mode" in
format)
	gofmt -w hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/tr027_spatial_source.go hat/hatSql/tr027_spatial_sql_test.go hat/hatSql/tr027_spatial_sql_benchmark_test.go
	;;
verify-docs)
	for path in TR027_RTREE_SPATIAL_INDEX.md README.md BENCHMARK.md INSPIRATION_BACKLOG.md; do
		test -s "$path"
	done
	rg -n 'RTreeSpatialSource|TR-027|benchmark-tr027-spatial-index|tr-027-spatial-r-tree-sql-index' TR027_RTREE_SPATIAL_INDEX.md README.md BENCHMARK.md INSPIRATION_BACKLOG.md
	;;
review)
	git diff --check -- Makefile BENCHMARK.md INSPIRATION_BACKLOG.md README.md TR027_RTREE_SPATIAL_INDEX.md hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/tr027_spatial_source.go hat/hatSql/tr027_spatial_sql_test.go hat/hatSql/tr027_spatial_sql_benchmark_test.go scripts/test-tr027-spatial-index.sh
	git status --short
	;;
stage)
	git add Makefile BENCHMARK.md INSPIRATION_BACKLOG.md README.md TR027_RTREE_SPATIAL_INDEX.md hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/tr027_spatial_source.go hat/hatSql/tr027_spatial_sql_test.go hat/hatSql/tr027_spatial_sql_benchmark_test.go scripts/test-tr027-spatial-index.sh
	git diff --cached --check
	git diff --cached --stat
	;;
commit)
	git commit -m "Add R-tree spatial SQL index [skip ci]"
	;;
push)
	git push origin HEAD:master
	;;
test)
	go test -v ./hat/hatSql -run '^TestTR027' -count=1
	;;
benchmark)
	go test -v ./hat/hatSql -run '^$' -bench '^BenchmarkTR027' -benchmem -count=5
	;;
race)
	go test -race ./hat/hatSql -run '^TestTR027' -count=1
	;;
vet)
	go vet ./hat/hatSql
	;;
*)
	printf 'unknown mode %q\n' "$mode" >&2
	exit 2
	;;
esac
