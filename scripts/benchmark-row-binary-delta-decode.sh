#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
worktree=/tmp/hatrie-cache-row-binary-delta-decode-benchmark

cleanup() {
	status=$?
	git -C "$repo_root" worktree remove --force "$worktree" >/dev/null 2>&1 || true
	exit "$status"
}
trap cleanup EXIT

git -C "$repo_root" worktree remove --force "$worktree" >/dev/null 2>&1 || true
git -C "$repo_root" worktree add --detach "$worktree" origin/codex/row-binary-bitmap-decode-into
cp "$repo_root/hat/hatSql/row_binary_adaptive_decoder.go" "$worktree/hat/hatSql/"
cp "$repo_root/hat/hatSql/row_binary_delta_codec.go" "$worktree/hat/hatSql/"
cp "$repo_root/hat/hatSql/row_binary_delta_benchmark_test.go" "$worktree/hat/hatSql/"
cp "$repo_root/hat/hatSql/row_binary_delta_decode_benchmark_test.go" "$worktree/hat/hatSql/"
cp "$repo_root/hat/hatSql/row_binary_delta_decode_into_benchmark_test.go" "$worktree/hat/hatSql/"
export GOCACHE="$worktree/.gocache"

(cd "$worktree" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRowBinary(Double)?DeltaDecode(Baseline|IntoReuse)$' -benchmem -count=10)
