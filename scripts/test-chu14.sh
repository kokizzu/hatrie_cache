#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
cache="${TMPDIR:-/tmp}/hatrie-cache-chu14-gocache"
rm -rf "$cache"
mkdir -p "$cache"
trap 'rm -rf "$cache"' EXIT

case "$mode" in
test)
	GOCACHE="$cache" go test ./hat/hatSql -run '^TestCHU14SpillRuntimeBloomFilterPreservesRowsAndReportsSkips$' -count=1
	;;
benchmark)
	GOCACHE="$cache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU14SpillRuntimeJoinFilter$' -benchmem -count=5
	;;
verify)
	pattern='^(TestCHU14|TestC229|TestRuntimeJoinBloomFilter|TestSQLSpillBloomFilters)'
	GOCACHE="$cache" go test ./hat/hatSql -run "$pattern" -count=1
	GOCACHE="$cache" go test -race ./hat/hatSql -run "$pattern" -count=1
	GOCACHE="$cache" go test -vet=all ./hat/hatSql -run "$pattern" -count=1
	rg -n -m 1 'CHU14_RUNTIME_JOIN_FILTER|chu14-spill-runtime-join-filter|CH-U14' CHU14_RUNTIME_JOIN_FILTER.md README.md BENCHMARK.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md
	git diff --check
	;;
package)
	GOCACHE="$cache" go test ./hat/hatSql -count=1
	;;
*)
	echo "unsupported mode: $mode" >&2
	exit 2
	;;
esac
