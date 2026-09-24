#!/usr/bin/env bash
set -euo pipefail

baseline_commit=4855231c
baseline_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-ch041-grouping-plan.XXXXXX")
cleanup() {
	printf 'cleaning comparison directory: %s\n' "$baseline_dir"
	rm -rf -- "$baseline_dir"
}
trap cleanup EXIT

git archive "$baseline_commit" | tar -x -C "$baseline_dir"
cp hat/hatSql/ch041_grouping_query_benchmark_test.go "$baseline_dir/hat/hatSql/ch041_grouping_query_benchmark_test.go"
printf '%s\n' "## baseline $baseline_commit"
(cd "$baseline_dir" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH041GroupingSetQuery$' -benchmem -count=10)
printf '%s\n' '## current worktree'
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH041GroupingSetQuery$' -benchmem -count=10
