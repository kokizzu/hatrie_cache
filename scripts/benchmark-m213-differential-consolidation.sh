#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
worktree="$(mktemp -d /tmp/hatrie-m213-benchmark.XXXXXX)"
cleanup() {
	git -C "$repo_root" worktree remove --force "$worktree" >/dev/null 2>&1 || true
	rm -rf "$worktree"
}
trap cleanup EXIT

git -C "$repo_root" fetch origin master
git -C "$repo_root" worktree add --detach "$worktree" origin/master
cp "$repo_root/hat/hatSql/m213_differential_consolidation.go" "$worktree/hat/hatSql/"
cp "$repo_root/hat/hatSql/m213_differential_consolidation_baseline_benchmark_test.go" "$worktree/hat/hatSql/"
cp "$repo_root/hat/hatSql/m213_differential_consolidation_benchmark_test.go" "$worktree/hat/hatSql/"

(
	cd "$worktree"
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkM213' -benchmem -count=5
)
