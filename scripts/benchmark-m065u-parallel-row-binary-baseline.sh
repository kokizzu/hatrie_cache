#!/usr/bin/env bash
set -euo pipefail

root=$(git rev-parse --show-toplevel)
worktree=/tmp/hatrie_cache_m065u_parallel_row_binary_baseline

git worktree remove --force "$worktree" >/dev/null 2>&1 || true
git worktree add --detach "$worktree" HEAD >/dev/null
trap 'git worktree remove --force "$worktree" >/dev/null 2>&1 || true' EXIT

cp "$root/hat/hatSql/row_binary_parallel_benchmark_test.go" "$worktree/hat/hatSql/row_binary_parallel_benchmark_test.go"
cd "$worktree"
GOMAXPROCS=8 go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRowBinaryParallelDecodeWorkload$' -benchmem -benchtime=250ms -count=7
