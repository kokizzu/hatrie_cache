#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md CHG14_PREPARED_PLAN_METRICS.md hat/hatSql/query.go hat/hatSql/prepared_cache_key.go hat/hatSql/chg14_plan_cache_metrics_test.go scripts/benchmark-chg14-before.sh scripts/benchmark-chg14-after.sh scripts/format-chg14.sh scripts/race-chg14.sh scripts/vet-chg14.sh scripts/test-chg14-package.sh scripts/review-chg14.sh scripts/commit-chg14.sh
git diff --cached --check
git commit -m 'feat: add prepared plan cache lifecycle metrics [skip ci]'
