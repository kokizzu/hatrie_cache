#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master

printf '%s\n' '== focused election-store tests =='
go test -run '^TestElectionStore' ./hat/hatTopology
printf '%s\n' '== full topology tests =='
go test ./hat/hatTopology
printf '%s\n' '== race topology tests =='
go test -race ./hat/hatTopology
printf '%s\n' '== CLI election path tests =='
go test -run '^TestRunElection' ./cmd/hatrie-cli
