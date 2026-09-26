#!/usr/bin/env bash
set -euo pipefail

test -f C153_PARTITION_OWNERSHIP_GRPC.md
rg -q 'PartitionOwnershipConsensusVote' proto/hatriecache/v1/cache.proto
rg -q 'PartitionOwnershipConsensusVote' internal/gen/hatriecache/v1/cache_grpc.pb.go
rg -q '^[-] \[x\] C153 ' INSPIRATION.md
rg -q '^## C153g Partition-ownership consensus vote transport' BENCHMARK.md
rg -q 'make test-c153g-all' C153_PARTITION_OWNERSHIP_GRPC.md
printf '%s\n' 'C153g documentation and generated RPC checks passed.'
