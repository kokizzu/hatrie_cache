#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master

printf '%s\n' '== focused replay-digest tests =='
go test -run '^TestDigestReplayRecords' ./hat/hatReplication
printf '%s\n' '== full replication tests =='
go test ./hat/hatReplication
printf '%s\n' '== race replication tests =='
go test -race ./hat/hatReplication
