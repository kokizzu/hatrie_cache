#!/bin/sh
set -eu
git diff --check
git status --short
git diff --stat -- Makefile README.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md CONNECTION_POOL.md hat/hatReplication/connection_pool.go hat/hatReplication/connection_pool_test.go hat/hatReplication/connection_pool_benchmark_test.go scripts/benchmark-connection-pool.sh scripts/test-connection-pool.sh scripts/format-connection-pool.sh scripts/test-connection-pool-package.sh scripts/race-connection-pool.sh scripts/vet-connection-pool.sh
