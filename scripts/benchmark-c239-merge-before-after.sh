#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(pwd)
worktree_dir=$(mktemp -d /tmp/hatrie-cache-c239-before-XXXXXX)
cleanup() {
	git worktree remove --force "$worktree_dir" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$worktree_dir" HEAD >/dev/null
cp "$repo_dir/hat/hatSql/c239_part_merge_metrics_baseline_benchmark_test.go" "$worktree_dir/hat/hatSql/"
(cd "$worktree_dir" && GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkC239PartMergeMetricsBaseline$' -benchmem -benchtime=2s -count=5)
GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkC239PartMergeMetrics$' -benchmem -benchtime=2s -count=5
