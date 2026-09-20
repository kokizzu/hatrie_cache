#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  BENCHMARK.md \
  INSPIRATION.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  TU28_CONNECTION_POOL_LIFECYCLE.md \
  hat/hatPeer/connection_pool.go \
  hat/hatPeer/peer_lifecycle.go \
  hat/hatPeer/tu28_connection_pool_lifecycle_test.go \
  hat/hatPeer/tu28_connection_pool_lifecycle_baseline_benchmark_test.go \
  scripts/benchmark-tu28-lifecycle.sh \
  scripts/format-tu28-lifecycle.sh \
  scripts/race-tu28-lifecycle.sh \
  scripts/test-tu28-lifecycle.sh \
	scripts/test-tu28-package.sh \
	scripts/vet-tu28-lifecycle.sh \
	scripts/verify-tu28.sh \
	scripts/commit-tu28.sh \
	scripts/push-tu28.sh
git commit -m "feat: integrate connection pool lifecycle hooks"
