#!/usr/bin/env bash
set -euo pipefail

root=$(pwd)
temporary_worktree=$(mktemp -d /tmp/hatrie-cache-mu016-baseline.XXXXXX)

cleanup() {
	if git worktree remove --force "$temporary_worktree"; then
		return
	fi
	rm -rf "$temporary_worktree"
}
trap cleanup EXIT

git worktree add --detach "$temporary_worktree" HEAD
cp "$root/hat/hatSql/mu016_transactional_view_baseline_benchmark_test.go" "$temporary_worktree/hat/hatSql/"
cd "$temporary_worktree"
GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkMU016ViewCreateBaseline$' -benchmem -benchtime=500ms -count=5
