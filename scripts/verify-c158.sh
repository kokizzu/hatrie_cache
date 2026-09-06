#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master

printf '%s\n' '== topology fencing tests =='
go test -run '^TestClusterTopology.*Fencing' ./hat/hatTopology
go test -race -run '^TestClusterTopology.*Fencing' ./hat/hatTopology
printf '%s\n' '== replication fencing tests =='
go test -run '^Test(ReplicationFencing|TopologyStoreRejectsRegressingFencing|ReplicationBatchEnvelopePreservesFencing|GRPCTopologyFencing)' ./hat/hatCache
go test -race -run '^Test(ReplicationFencing|TopologyStoreRejectsRegressingFencing|ReplicationBatchEnvelopePreservesFencing|GRPCTopologyFencing)' ./hat/hatCache
