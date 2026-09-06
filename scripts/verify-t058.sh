#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)

cleanup() {
	git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

git -C "$repo" worktree add --detach "$tmp" origin/master >/dev/null
cp "$repo/hat/hatReplication/quorum_policy_read.go" "$tmp/hat/hatReplication/"
cp "$repo/hat/hatReplication/quorum_policy_read_test.go" "$tmp/hat/hatReplication/"
cp "$repo/hat/hatReplication/quorum_policy_read_edge_test.go" "$tmp/hat/hatReplication/"

cd "$tmp"
go test -run '^TestQuorumPolicy' ./hat/hatReplication
go test ./hat/hatReplication
go test -race ./hat/hatReplication
