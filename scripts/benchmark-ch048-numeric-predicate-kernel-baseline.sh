#!/usr/bin/env bash
set -euo pipefail

root=$(git rev-parse --show-toplevel)
worktree=/tmp/hatrie_cache_ch048_numeric_predicate_baseline

git worktree remove --force "$worktree" >/dev/null 2>&1 || true
git worktree add --detach "$worktree" HEAD >/dev/null
trap 'git worktree remove --force "$worktree" >/dev/null 2>&1 || true' EXIT

cd "$worktree"
GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableColumnarCompressedBatchesQuery/(legacy|compressed)$' -benchmem -benchtime=250ms -count=7
