#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
format)
	gofmt -w hat/hatSql/query.go hat/hatSchema/materialized.go hat/hatSchema/tr030_index_stats_test.go hat/hatSchema/tr030_index_stats_benchmark_test.go
	;;
test)
	go test ./hat/hatSchema -run 'TestTR030'
	;;
benchmark)
	go test ./hat/hatSchema -run '^$' -bench 'BenchmarkTR030' -benchmem -count=5
	;;
race)
	go test -race ./hat/hatSchema -run 'TestTR030'
	;;
vet)
	go vet ./hat/hatSchema
	;;
package)
	go test ./hat/hatSchema
	;;
review)
	git diff --check
	go test ./hat/hatSchema -run 'TestTR030'
	;;
stage)
	git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md INSPIRATION_BACKLOG.md Makefile README.md TR030_INDEX_STATS.md hat/hatSql/query.go hat/hatSchema/materialized.go hat/hatSchema/tr030_index_stats_test.go hat/hatSchema/tr030_index_stats_benchmark_test.go scripts/run-tr030-index-stats.sh
	;;
commit)
	git commit -m "feat: add materialized index statistics [skip ci]"
	;;
push)
	git push origin HEAD:master
	;;
status)
	git status --short --branch
	;;
*)
	printf 'unknown mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
