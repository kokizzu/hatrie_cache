#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
paths=(
	Makefile
	scripts/run-chu64-count-field.sh
	scripts/ship-chu64-count-field.sh
	hat/hatSql/columnar_count_field.go
	hat/hatSql/query.go
	hat/hatSql/chu64_count_field_test.go
	hat/hatSql/chu64_count_field_benchmark_test.go
	CHU64_COLUMNAR_COUNT_FIELD.md
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
	git -C "$root_dir" commit -m 'feat(sql): add columnar COUNT field metadata [skip ci]'
	git -C "$root_dir" push -u origin "$(git -C "$root_dir" branch --show-current)"
	;;
  *)
	printf 'usage: %s {review|ship}\n' "$0" >&2
	exit 2
	;;
esac
