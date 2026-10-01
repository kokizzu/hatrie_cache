#!/usr/bin/env bash
set -euo pipefail

mode=${1:-review}
paths=(
	Makefile
	scripts/run-chu66-grouped-numeric-aggregate.sh
	scripts/ship-chu66-grouped-numeric-aggregate.sh
	hat/hatSql/columnar_numeric_aggregate.go
	hat/hatSql/query.go
	hat/hatSql/chu66_grouped_numeric_aggregate_test.go
	hat/hatSql/chu66_grouped_numeric_aggregate_benchmark_test.go
	CHU66_GROUPED_NUMERIC_AGGREGATE.md
	SQL_IMPROVEMENTS_100.md
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
)

if [[ -e hat/hatSql/round25_compat.go || -e hat/hatSql/chu66_baseline_benchmark_test.go || -e scripts/inspect-chu66-source.sh ]]; then
	printf '%s\n' 'temporary CHU66 scaffolding remains' >&2
	exit 1
fi
if grep -R -n 'CHU66 packed aggregate path reached\|inspect-chu66-source\|benchmark-chu66-grouped-numeric-aggregate-baseline' hat/hatSql scripts/run-chu66-grouped-numeric-aggregate.sh Makefile >/dev/null 2>&1; then
	printf '%s\n' 'temporary CHU66 debug/scaffold reference remains' >&2
	exit 1
fi

git diff --check

case "$mode" in
review)
	git diff --stat -- "${paths[@]}"
	git status --short -- "${paths[@]}"
	;;
ship)
	git add -- "${paths[@]}"
	git diff --cached --check
	git commit -m 'feat(sql): vectorize grouped numeric aggregates [skip ci]'
	git push -u origin HEAD
	;;
*)
	printf 'usage: %s review|ship\n' "$0" >&2
	exit 2
	;;
esac
