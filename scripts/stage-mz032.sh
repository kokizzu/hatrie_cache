#!/bin/sh
set -eu

git add BENCHMARK.md INSPIRATION_BACKLOG.md MZ032_ARRANGEMENT_COST.md README.md Makefile hat/hatSql/arrangement_cost.go hat/hatSql/mz032_arrangement_cost_benchmark_test.go hat/hatSql/mz032_arrangement_cost_test.go scripts/benchmark-mz032-baseline.sh scripts/benchmark-mz032.sh scripts/format-mz032.sh scripts/test-mz032.sh scripts/verify-mz032.sh scripts/stage-mz032.sh scripts/review-mz032-staged.sh scripts/commit-mz032.sh scripts/push-mz032.sh
