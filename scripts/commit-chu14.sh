#!/usr/bin/env bash
set -euo pipefail

git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md CHU14_RUNTIME_JOIN_FILTER.md Makefile PRODUCT_IDEA_GAPS.md README.md hat/hatSql/query.go hat/hatSql/chu14_spill_runtime_filter_benchmark_test.go hat/hatSql/chu14_spill_runtime_filter_test.go scripts/commit-chu14.sh scripts/format-chu14.sh scripts/push-chu14.sh scripts/test-chu14.sh
git diff --cached --check
git commit -m "hatSql: propagate spill join runtime filters"
