#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatTopology/c153f_partition_ownership_consensus_collector.go \
  hat/hatTopology/c153f_partition_ownership_consensus_collector_test.go \
  hat/hatTopology/c153f_partition_ownership_consensus_collector_benchmark_test.go
