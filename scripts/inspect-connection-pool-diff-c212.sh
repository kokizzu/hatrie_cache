#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --stat -- CONNECTION_POOL.md CONNECTION_POOL_IDLE_FASTPATH.md BENCHMARK.md INSPIRATION.md hat/hatReplication/connection_pool.go hat/hatReplication/connection_pool_idle_fastpath_test.go scripts/test-connection-pool-c212.sh scripts/benchmark-connection-pool-c212.sh scripts/format-connection-pool-c212.sh scripts/verify-connection-pool-c212.sh scripts/stage-connection-pool-c212.sh scripts/commit-connection-pool-c212.sh scripts/push-connection-pool-c212.sh
git diff -- hat/hatReplication/connection_pool.go BENCHMARK.md CONNECTION_POOL.md INSPIRATION.md
git diff --check -- CONNECTION_POOL.md CONNECTION_POOL_IDLE_FASTPATH.md BENCHMARK.md INSPIRATION.md hat/hatReplication/connection_pool.go hat/hatReplication/connection_pool_idle_fastpath_test.go scripts/test-connection-pool-c212.sh scripts/benchmark-connection-pool-c212.sh scripts/format-connection-pool-c212.sh scripts/verify-connection-pool-c212.sh scripts/stage-connection-pool-c212.sh scripts/commit-connection-pool-c212.sh scripts/push-connection-pool-c212.sh
git diff --cached --stat
git diff --cached -- Makefile
