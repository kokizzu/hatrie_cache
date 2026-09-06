#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master

printf '%s\n' '== focused object-store backup tests =='
go test -run '^TestObjectStoreTarget' ./hat/hatBackup
printf '%s\n' '== full hatBackup tests =='
go test ./hat/hatBackup
printf '%s\n' '== race hatBackup tests =='
go test -race ./hat/hatBackup
