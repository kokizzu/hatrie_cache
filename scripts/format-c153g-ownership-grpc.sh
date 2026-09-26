#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/c153g_partition_ownership_consensus_grpc.go \
  hat/hatCache/c153g_partition_ownership_consensus_grpc_test.go \
  hat/hatCache/c153g_partition_ownership_consensus_grpc_benchmark_test.go
