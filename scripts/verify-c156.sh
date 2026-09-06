#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master

printf '%s\n' '== regional topology tests =='
go test -run '^TestTopologyRegion' ./hat/hatTopology
go test -race -run '^TestTopologyRegion' ./hat/hatTopology
printf '%s\n' '== regional replication policy tests =='
go test -run '^TestReplicationRegion' ./hat/hatCache
go test -race -run '^TestReplicationRegion' ./hat/hatCache
