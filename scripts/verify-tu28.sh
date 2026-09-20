#!/usr/bin/env bash
set -euo pipefail

test -s TU28_CONNECTION_POOL_LIFECYCLE.md
rg -n 'T-U28 Connection Pool Lifecycle|TU28_CONNECTION_POOL_LIFECYCLE|BenchmarkTU28PoolDialEachCallWithLifecycle|MaxPeerLifecycleErrorBytes' \
  TU28_CONNECTION_POOL_LIFECYCLE.md BENCHMARK.md README.md INSPIRATION.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md \
  hat/hatPeer/connection_pool.go hat/hatPeer/peer_lifecycle.go
git diff --check
git status --short
git diff --stat
