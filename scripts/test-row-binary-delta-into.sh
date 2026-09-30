#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
worktree=/tmp/hatrie-cache-row-binary-delta-into
base=origin/codex/row-binary-bitmap-decode-into

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
cp "$repo_root/hat/hatSql/row_binary_delta_codec.go" "$worktree/hat/hatSql/"
cp "$repo_root/hat/hatSql/row_binary_delta_benchmark_test.go" "$worktree/hat/hatSql/"
cp "$repo_root/hat/hatSql/row_binary_delta_into_test.go" "$worktree/hat/hatSql/"
export GOCACHE="$worktree/.gocache"

(cd "$worktree" && go test ./hat/hatSql -run '^TestSQLRowBinaryDeltaEncodeIntoMatchesBothFormats$')
