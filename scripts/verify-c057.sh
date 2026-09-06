#!/usr/bin/env bash
set -euo pipefail

root_dir="$(git rev-parse --show-toplevel)"
tmp_dir="$(mktemp -d)"
worktree_dir="$tmp_dir/worktree"

cleanup() {
	git -C "$root_dir" worktree remove --force "$worktree_dir" >/dev/null 2>&1 || true
	rm -rf "$tmp_dir"
}
trap cleanup EXIT

git -C "$root_dir" worktree add --detach "$worktree_dir" HEAD
cd "$worktree_dir"
go test ./hat/hatCodec -run '^TestGorillaFloat64'
go test ./hat/hatCodec
go test -race ./hat/hatCodec
