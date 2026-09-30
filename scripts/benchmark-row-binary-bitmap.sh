#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
worktree=/tmp/hatrie-cache-row-binary-bitmap-after
base=origin/codex/row-binary-dictionary-decode-into

cleanup() {
    status=$?
    if [ -e "$worktree" ]; then
        git -C "$repo_root" worktree remove --force "$worktree" || true
    fi
    exit "$status"
}
trap cleanup EXIT

if [ -e "$worktree" ]; then
    git -C "$repo_root" worktree remove --force "$worktree"
fi
git -C "$repo_root" worktree add --detach "$worktree" "$base"
cp "$repo_root/hat/hatSql/row_binary_nullable_bitmap.go" "$worktree/hat/hatSql/"
cp "$repo_root/hat/hatSql/row_binary_bitmap_benchmark_test.go" "$worktree/hat/hatSql/"
cp "$repo_root/hat/hatSql/row_binary_bitmap_into_benchmark_test.go" "$worktree/hat/hatSql/"
export GOCACHE="$worktree/.gocache"

(cd "$worktree" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRowBinaryBitmapEncode(Baseline|IntoReuse)$' -benchmem -count=10)
