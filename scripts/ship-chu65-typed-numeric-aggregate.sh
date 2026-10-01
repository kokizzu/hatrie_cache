#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
paths=(
	Makefile
	scripts/run-chu65-typed-numeric-aggregate.sh
	scripts/ship-chu65-typed-numeric-aggregate.sh
	hat/hatSql/columnar_numeric_aggregate.go
	hat/hatSql/query.go
	hat/hatSql/chu65_typed_numeric_aggregate_test.go
	hat/hatSql/chu65_typed_numeric_aggregate_benchmark_test.go
	CHU65_TYPED_NUMERIC_AGGREGATE.md
	SQL_IMPROVEMENTS_100.md
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
)

case "${1:-review}" in
  review)
	git -C "$root_dir" diff --check
	git -C "$root_dir" status --short
	;;
  ship)
	git -C "$root_dir" add "${paths[@]}"
	git -C "$root_dir" diff --cached --check
	git -C "$root_dir" commit -m 'feat(sql): add typed numeric aggregate kernel [skip ci]'
	git -C "$root_dir" push -u origin "$(git -C "$root_dir" branch --show-current)"
	;;
  *)
	printf 'usage: %s {review|ship}\n' "$0" >&2
	exit 2
	;;
esac
