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
cp "$root_dir/hat/hatDataStructure/rtree.go" "$worktree_dir/hat/hatDataStructure/rtree.go"
cp "$root_dir/hat/hatDataStructure/rtree_test.go" "$worktree_dir/hat/hatDataStructure/rtree_test.go"
cp "$root_dir/hat/hatDataStructure/rtree_public_test.go" "$worktree_dir/hat/hatDataStructure/rtree_public_test.go"

cd "$worktree_dir"
gofmt -w hat/hatDataStructure/rtree.go hat/hatDataStructure/rtree_test.go hat/hatDataStructure/rtree_public_test.go
go test -race ./hat/hatDataStructure -run 'TestRTree'
go test -race ./hat/hatDataStructure
