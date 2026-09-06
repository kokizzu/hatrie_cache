#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master

printf '%s\n' '== focused collection metrics tests =='
go test -run '^TestCollection' ./hat/hatMetrics
printf '%s\n' '== full hatMetrics tests =='
go test ./hat/hatMetrics
printf '%s\n' '== race hatMetrics tests =='
go test -race ./hat/hatMetrics
