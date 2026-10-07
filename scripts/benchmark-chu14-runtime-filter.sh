#!/usr/bin/env bash
set -euo pipefail

mode="${1:-benchmark}"
count="${BENCHMARK_COUNT:-5}"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-chu14.XXXXXX")"
cleanup() {
	rm -rf -- "$tmp_dir"
}
trap cleanup EXIT INT TERM
mkdir -p "$tmp_dir/cache" "$tmp_dir/tmp"
export GOCACHE="$tmp_dir/cache"
export GOTMPDIR="$tmp_dir/tmp"

case "$mode" in
benchmark)
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRuntimeJoinFilter$' -benchmem -count="$count"
	;;
test)
	go test ./hat/hatSql -run '^TestRuntimeJoinBloomFilter' -count=1
	;;
race)
	go test -race ./hat/hatSql -run '^TestRuntimeJoinBloomFilter' -count=1
	;;
vet)
	go vet ./hat/hatSql
	;;
*)
	printf 'usage: %s benchmark|test|race|vet\n' "$0" >&2
	exit 2
	;;
esac
