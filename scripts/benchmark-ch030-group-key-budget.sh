#!/usr/bin/env bash
set -euo pipefail

repo=$(pwd)
baseline=$(mktemp -d)
trap 'rm -rf "$baseline"' EXIT
git archive HEAD | tar -x -C "$baseline"
cp "$repo/hat/hatSql/max_group_keys_benchmark_test.go" "$baseline/hat/hatSql/"
case "${1:-compare}" in
baseline)
	printf '%s\n' '===== baseline HEAD ====='
	(cd "$baseline" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLGroupRowsDefault$' -benchmem -count=7)
	;;
current)
	printf '%s\n' '===== current worktree ====='
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLGroupRows(Default|WithKeyLimit)$' -benchmem -count=7
	;;
compare)
	printf '%s\n' '===== baseline HEAD ====='
	(cd "$baseline" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLGroupRowsDefault$' -benchmem -count=7)
	printf '%s\n' '===== current worktree ====='
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLGroupRows(Default|WithKeyLimit)$' -benchmem -count=7
	;;
*)
	printf 'unknown benchmark mode: %s\n' "$1" >&2
	exit 2
	;;
esac
