#!/usr/bin/env bash
set -euo pipefail

git add -- \
    BENCHMARK.md \
    MZ045_COMPILED_PLAN_EQUIVALENCE.md \
    Makefile \
    hat/hatSql/mz045_arrangement_recommendation_cache.go \
    hat/hatSql/mz045_arrangement_recommendation_cache_baseline_benchmark_test.go \
    hat/hatSql/mz045_arrangement_recommendation_cache_public_test.go \
    hat/hatSql/mz045_arrangement_recommendation_cache_test.go \
    scripts/benchmark-mz045-arrangement-cache-before.sh \
    scripts/benchmark-mz045-arrangement-cache.sh \
    scripts/commit-mz045-arrangement-cache.sh \
    scripts/format-mz045-arrangement-cache.sh \
    scripts/push-mz045-arrangement-cache.sh \
    scripts/race-mz045-arrangement-cache.sh \
    scripts/stage-mz045-arrangement-cache.sh \
    scripts/status-mz045-arrangement-cache.sh \
    scripts/test-mz045-arrangement-cache.sh \
    scripts/vet-mz045-arrangement-cache.sh

git diff --cached --check
git diff --cached --stat
