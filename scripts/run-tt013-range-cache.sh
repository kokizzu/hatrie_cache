#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
	format)
		gofmt -w hat/hatDataStructure/tt013_range_cache.go hat/hatDataStructure/tt013_range_cache_test.go hat/hatDataStructure/tt013_range_cache_benchmark_test.go
		;;
	test)
		go test ./hat/hatDataStructure -run '^TestTT013OrderedIndexRangeCache' -count=1
		;;
	test-package)
		go test ./hat/hatDataStructure -count=1
		;;
	race)
		go test -race ./hat/hatDataStructure -run '^TestTT013OrderedIndexRangeCache' -count=1
		;;
	benchmark)
		go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTT013Range' -benchmem -count=5
		;;
	vet)
		go vet ./hat/hatDataStructure
		;;
	review)
		git diff --check
		git diff --cached --check
		git diff --cached --stat
		git status --short
		;;
	stage)
		git add BENCHMARK.md ENGINE_IDEAS.md README.md TT013_RANGE_TUPLE_CACHE.md Makefile hat/hatDataStructure/tt013_range_cache.go hat/hatDataStructure/tt013_range_cache_test.go hat/hatDataStructure/tt013_range_cache_benchmark_test.go scripts/run-tt013-range-cache.sh
		;;
	commit)
		git commit -m 'feat(data-structure): add bounded ordered range cache [skip ci]'
		;;
	push)
		git push -u origin HEAD
		;;
	*)
		printf '%s\n' 'usage: format|test|test-package|race|benchmark|vet|review|stage|commit|push'
		exit 2
		;;
esac
