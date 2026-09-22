#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
baseline_dir=$(mktemp -d /tmp/hatrie-c238-baseline.XXXXXX)
cleanup() {
	git worktree remove --force "$baseline_dir" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$baseline_dir" HEAD~1 >/dev/null
cp "$repo_root/hat/hatSql/c238_mutation_queue_progress_baseline_benchmark_test.go" "$baseline_dir/hat/hatSql/"
printf '%s\n' 'C237 baseline benchmark:'
(cd "$baseline_dir" && GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkC238MutationQueueProgressBaseline$' -benchmem -benchtime=2s -count=5)
printf '%s\n' 'C238 progress benchmark:'
GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkC238MutationQueueProgress$' -benchmem -benchtime=2s -count=5
