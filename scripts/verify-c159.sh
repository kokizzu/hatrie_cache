#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master

printf '%s\n' '== topology failure-domain tests =='
go test -run '^TestFailureDomainPlacementValidation$' ./hat/hatTopology
go test -race -run '^TestFailureDomainPlacementValidation$' ./hat/hatTopology
printf '%s\n' '== CLI placement tests =='
go test -run '^TestClusterReplicaPlacementHonorsFailureDomains$' ./cmd/hatrie-cli
go test -race -run '^TestClusterReplicaPlacementHonorsFailureDomains$' ./cmd/hatrie-cli
