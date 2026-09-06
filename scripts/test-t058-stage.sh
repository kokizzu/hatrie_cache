#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master
cp "$repo/hat/hatReplication/quorum_policy_read_test.go" "$tmp/hat/hatReplication/quorum_policy_read_test.go"
go test -run '^TestQuorumPolicy' ./hat/hatReplication
