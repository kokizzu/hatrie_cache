#!/usr/bin/env bash
set -euo pipefail

baseline_dir=$(mktemp -d /tmp/hatrie-cache-ch041-baseline.XXXXXX)
baseline_ref=${BASELINE_REF:-HEAD^}
cleanup() {
	git worktree remove --force "$baseline_dir" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$baseline_dir" "$baseline_ref"
cp hat/hatSql/grouping_identifier_benchmark_test.go "$baseline_dir/hat/hatSql/grouping_identifier_benchmark_test.go"
(cd "$baseline_dir" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLGroupingSetIdentifiers/grouping_sets$' -benchmem -count=10)
