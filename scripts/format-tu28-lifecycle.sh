#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPeer/peer_lifecycle.go hat/hatPeer/connection_pool.go \
  hat/hatPeer/connection_pool_lifecycle_hooks_test.go \
  hat/hatPeer/connection_pool_lifecycle_hooks_benchmark_test.go
