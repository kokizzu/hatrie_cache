#!/bin/sh
set -eu
git add Makefile README.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md CONNECTION_POOL.md hat/hatReplication/connection_pool.go hat/hatReplication/connection_pool_test.go hat/hatReplication/connection_pool_benchmark_test.go scripts/benchmark-connection-pool.sh scripts/test-connection-pool.sh scripts/format-connection-pool.sh scripts/test-connection-pool-package.sh scripts/race-connection-pool.sh scripts/vet-connection-pool.sh scripts/review-connection-pool.sh scripts/commit-connection-pool.sh scripts/push-connection-pool.sh
git commit -m "feat: add connection pool"
