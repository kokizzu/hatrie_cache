#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master

printf '%s\n' '== focused compression negotiation tests =='
go test -run '^TestCompressionLevelPolicy' ./hat/hatCodec
printf '%s\n' '== full hatCodec tests =='
go test ./hat/hatCodec
printf '%s\n' '== race hatCodec tests =='
go test -race ./hat/hatCodec
