#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPeer/connection_pool.go \
  hat/hatPeer/peer_lifecycle.go \
  hat/hatPeer/tu28_connection_pool_lifecycle_test.go \
  hat/hatPeer/tu28_connection_pool_lifecycle_baseline_benchmark_test.go
