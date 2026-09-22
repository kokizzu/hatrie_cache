#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
baseline_dir=$(mktemp -d /tmp/hatrie-c237-baseline.XXXXXX)
cleanup() {
	git worktree remove --force "$baseline_dir" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$baseline_dir" HEAD >/dev/null
cp "$repo_root/hat/hatSql/c237_explain_projection_benchmark_test.go" "$baseline_dir/hat/hatSql/"
printf '%s\n' 'C230 baseline benchmark (before C237):'
(cd "$baseline_dir" && GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkC237ExplainProjectionSelection$' -benchmem -benchtime=2s -count=5)
