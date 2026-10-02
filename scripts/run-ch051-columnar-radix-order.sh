#!/usr/bin/env bash
set -euo pipefail

repo="$PWD"
source_file="$repo/hat/hatSql/typed_table_columnar_order.go"
test_file="$repo/hat/hatSql/round14_columnar_int64_order_test.go"
harness_file="$repo/scripts/ch051_columnar_radix_harness_test.go"
scratch=/tmp/hatrie-cache-ch051-columnar-radix-harness

prepare_harness() {
	rm -rf "$scratch"
	mkdir -p "$scratch"
	cp "$source_file" "$scratch/typed_table_columnar_order.go"
	cp "$test_file" "$scratch/round14_columnar_int64_order_test.go"
	cp "$harness_file" "$scratch/ch051_columnar_radix_harness_test.go"
	cd "$scratch"
}

cleanup() {
	rm -rf "$scratch"
}

case "${1:-}" in
	format)
		gofmt -w "$source_file" "$test_file" "$harness_file"
		;;
	test)
		prepare_harness
	trap cleanup EXIT
		GO111MODULE=off go test -count=1 -run '^TestRound14'
		;;
	race)
		prepare_harness
	trap cleanup EXIT
		GO111MODULE=off go test -race -count=1 -run '^TestRound14'
		;;
	benchmark)
		prepare_harness
	trap cleanup EXIT
		GO111MODULE=off go test -run '^$' -bench '^BenchmarkRound14TypedTableColumnarInt64Order$' -benchmem -count=5
		;;
	vet)
		prepare_harness
		trap cleanup EXIT
		GO111MODULE=off go vet .
		;;
	review)
		git diff --check
		git diff --cached --check
		git diff --cached --stat
		git diff --cached --unified=2 -- CH051_COLUMNAR_RADIX_ORDER.md BENCHMARK.md ENGINE_IDEAS.md README.md hat/hatSql/typed_table_columnar_order.go
		git status --short
		;;
	cleanup)
		rm -f "$repo/hat/hatSql/round14_harness_stubs_test.go"
		rm -rf "$scratch"
		;;
	stage)
		git add CH051_COLUMNAR_RADIX_ORDER.md BENCHMARK.md ENGINE_IDEAS.md README.md hat/hatSql/round14_columnar_int64_order_test.go hat/hatSql/typed_table_columnar_order.go scripts/ch051_columnar_radix_harness_test.go scripts/run-ch051-columnar-radix-order.sh Makefile
		;;
	commit)
		git commit -m 'perf(sql): use radix order for typed int64 columns [skip ci]'
		;;
	push)
		git push -u origin HEAD
		;;
	*)
		printf '%s\n' 'usage: format|test|race|benchmark|vet|review|stage|commit|push'
		exit 2
		;;
esac
