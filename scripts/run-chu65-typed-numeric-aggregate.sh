#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="/tmp/hatrie-cache-next-inspiration-round24-chu65"
rm -rf "$tmp_dir"
mkdir -p "$tmp_dir/cache" "$tmp_dir/tmp"
cleanup() {
	rm -rf "$tmp_dir"
}
trap cleanup EXIT

export GOCACHE="$tmp_dir/cache"
export GOTMPDIR="$tmp_dir/tmp"

case "${1:-test}" in
  format)
	gofmt -w "$root_dir/hat/hatSql/columnar_numeric_aggregate.go" "$root_dir/hat/hatSql/query.go" "$root_dir/hat/hatSql/chu65_typed_numeric_aggregate_test.go" "$root_dir/hat/hatSql/chu65_typed_numeric_aggregate_benchmark_test.go"
	;;
  test)
	go test ./hat/hatSql -run '^TestSQLColumnar(Packed|TypedNumeric)'
	;;
  package)
	go test ./hat/hatSql
	;;
  benchmark)
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLColumnarTypedNumericAggregates$' -benchmem -count=5
	;;
  race)
	go test -race ./hat/hatSql -run '^TestSQLColumnar(Packed|TypedNumeric)'
	;;
  vet)
	go vet ./hat/hatSql
	;;
  *)
printf 'usage: %s {format|test|package|benchmark|race|vet}\n' "$0" >&2
	exit 2
	;;
esac
