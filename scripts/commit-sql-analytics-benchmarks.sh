#!/usr/bin/env sh
set -eu

make review-sql-analytics-benchmarks
git add -- Makefile hat/hatSql/graph.go hat/hatSql/analytics_benchmark_test.go scripts/benchmark-sql-analytics-goal.sh scripts/format-sql-analytics-benchmarks.sh scripts/review-sql-analytics-benchmarks.sh scripts/commit-sql-analytics-benchmarks.sh
git diff --cached --check
git commit --only -m 'perf: benchmark SQL analytics helpers' -- Makefile hat/hatSql/graph.go hat/hatSql/analytics_benchmark_test.go scripts/benchmark-sql-analytics-goal.sh scripts/format-sql-analytics-benchmarks.sh scripts/review-sql-analytics-benchmarks.sh scripts/commit-sql-analytics-benchmarks.sh
git push
