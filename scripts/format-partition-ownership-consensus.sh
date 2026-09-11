#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatTopology/ownership_consensus.go \
  hat/hatTopology/ownership_consensus_benchmark_test.go \
  hat/hatTopology/ownership_consensus_test.go
