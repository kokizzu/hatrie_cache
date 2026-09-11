#!/usr/bin/env bash
set -euo pipefail

worktree="$(mktemp -d)"
cleanup() {
	git worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$worktree" HEAD
cp hat/hatSql/materialized_dependency_benchmark_test.go "$worktree/hat/hatSql/materialized_dependency_benchmark_test.go"
(
	cd "$worktree"
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ042MaterializedRefreshChanged$' -benchmem -benchtime=1s -count=5
)
