#!/usr/bin/env bash
set -euo pipefail

repo=$(pwd)
baseline=$(mktemp -d)
trap 'rm -rf "$baseline"' EXIT
git archive HEAD | tar -x -C "$baseline"
cp "$repo/hat/hatSql/asof_join_benchmark_test.go" "$baseline/hat/hatSql/"
case "${1:-compare}" in
baseline)
	printf '%s\n' '===== baseline HEAD: nested nearest-match scan ====='
	(cd "$baseline" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLAsofJoinBaseline$' -benchmem -count=7)
	;;
current)
	printf '%s\n' '===== current worktree: nested and bucketed nearest-match scan ====='
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLAsofJoin(Baseline|Optimized)$' -benchmem -count=7
	;;
compare)
	printf '%s\n' '===== baseline HEAD: nested nearest-match scan ====='
	(cd "$baseline" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLAsofJoinBaseline$' -benchmem -count=7)
	printf '%s\n' '===== current worktree: nested and bucketed nearest-match scan ====='
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLAsofJoin(Baseline|Optimized)$' -benchmem -count=7
	;;
*)
	printf 'unknown benchmark mode: %s\n' "$1" >&2
	exit 2
	;;
esac
