#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add Makefile README.md BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md SQL_SOURCE_FRONTIERS.md sql_source_frontier_api.go hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/sql_source_frontier_requirement.go hat/hatSql/mz007_frontier_requirement_test.go hat/hatSql/m066_mz007_frontier_requirement_baseline_benchmark_test.go scripts/benchmark-mz007-frontier-rejection.sh scripts/test-mz007-frontier-rejection.sh scripts/test-race-mz007-frontier-rejection.sh scripts/format-mz007-frontier-rejection.sh scripts/verify-mz007-docs.sh scripts/review-mz007-frontier-rejection.sh scripts/commit-mz007-frontier-rejection.sh scripts/push-mz007-frontier-rejection.sh
git diff --cached --check
git commit -m "Add opt-in SQL source frontier rejection"
