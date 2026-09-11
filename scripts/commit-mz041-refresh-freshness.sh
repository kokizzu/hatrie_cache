#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md MANAGED_REFRESH_FRESHNESS.md hat/hatSql/refresh_scheduler.go hat/hatSql/refresh_scheduler_test.go hat/hatSql/refresh_scheduler_freshness_benchmark_test.go scripts/test-mz041-refresh-freshness.sh scripts/format-mz041-refresh-freshness.sh scripts/benchmark-mz041-refresh-freshness.sh scripts/verify-mz041-docs.sh scripts/test-race-mz041-refresh-freshness.sh scripts/vet-mz041-refresh-freshness.sh scripts/review-mz041-refresh-freshness.sh scripts/commit-mz041-refresh-freshness.sh scripts/push-mz041-refresh-freshness.sh
git diff --cached --check
git commit -m 'hatSql: add managed refresh freshness status'
