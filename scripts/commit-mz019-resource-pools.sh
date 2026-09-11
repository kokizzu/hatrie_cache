#!/usr/bin/env bash
set -euo pipefail

git add \
    ADOPTED_QUERY_ENGINE_IDEAS.md \
    BENCHMARK.md \
    ENGINE_IDEAS.md \
    Makefile \
    README.md \
    SQL_NAMESPACE_COMPUTE_POOLS.md \
    hat/hatCache/sql_query.go \
    hat/hatSql/governance.go \
    hat/hatSql/mz019_resource_pool_benchmark_test.go \
    hat/hatSql/mz019_resource_pool_test.go \
    scripts/benchmark-mz019-resource-pools.sh \
    scripts/commit-mz019-resource-pools.sh \
    scripts/format-mz019-resource-pools.sh \
    scripts/push-mz019-resource-pools.sh \
    scripts/review-mz019-resource-pools.sh \
    scripts/status-mz019-resource-pools.sh \
    scripts/test-mz019-broad.sh \
    scripts/test-mz019-resource-pools.sh \
    scripts/test-race-mz019-resource-pools.sh \
    scripts/verify-mz019-resource-pool-docs.sh
git commit -m 'adopt named SQL compute pools'
