#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
format)
	gofmt -w hat/hatSchema/materialized.go hat/hatSchema/tr023_functional_index_test.go hat/hatSchema/tr023_functional_index_baseline_benchmark_test.go hat/hatSchema/tr023_functional_index_benchmark_test.go
	;;
test)
	go test ./hat/hatSchema -run 'TestTR023'
	;;
benchmark)
	go test ./hat/hatSchema -run '^$' -bench 'BenchmarkTR023' -benchmem -count=5
	;;
race)
	go test -race ./hat/hatSchema -run 'TestTR023'
	;;
vet)
	go vet ./hat/hatSchema
	;;
package)
	go test ./hat/hatSchema
	;;
full-test)
	go test ./...
	;;
review)
	git diff --check
	go test ./hat/hatSchema -run 'TestTR023'
	;;
stage)
	git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md FUNCTIONAL_INDEX.md INSPIRATION_BACKLOG.md Makefile README.md TR023_FUNCTIONAL_INDEX.md hat/hatSchema/materialized.go hat/hatSchema/tr023_functional_index_baseline_benchmark_test.go hat/hatSchema/tr023_functional_index_benchmark_test.go hat/hatSchema/tr023_functional_index_test.go scripts/run-tr023-functional-index.sh
	;;
commit)
	git commit -m "feat: add materialized functional indexes [skip ci]"
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
