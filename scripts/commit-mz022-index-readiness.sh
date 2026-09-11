#!/usr/bin/env bash
set -euo pipefail

git add \
    ADOPTED_QUERY_ENGINE_IDEAS.md \
    BENCHMARK.md \
    ENGINE_IDEAS.md \
    Makefile \
    README.md \
    SQL_JSON_INDEX_READINESS.md \
    hat/hatCache/mz022_index_readiness_benchmark_test.go \
    hat/hatCache/mz022_index_readiness_test.go \
    hat/hatCache/sql_index_readiness.go \
    scripts/benchmark-mz022-index-readiness.sh \
    scripts/commit-mz022-index-readiness.sh \
    scripts/format-mz022-index-readiness.sh \
    scripts/push-mz022-index-readiness.sh \
    scripts/review-mz022-index-readiness.sh \
    scripts/status-mz022-index-readiness.sh \
    scripts/test-mz022-broad.sh \
    scripts/test-mz022-index-readiness.sh \
    scripts/test-race-mz022-index-readiness.sh \
    scripts/verify-mz022-index-readiness-docs.sh
git commit -m 'add SQL JSON index readiness barrier'
