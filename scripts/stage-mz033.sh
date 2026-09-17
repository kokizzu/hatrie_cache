#!/bin/sh
set -eu

git add BENCHMARK.md INSPIRATION_BACKLOG.md MZ033_DATAFLOW_INDEX_ADVISOR.md README.md Makefile hat/hatSql/dataflow_index_advisor.go hat/hatSql/mz033_dataflow_index_advisor_benchmark_test.go hat/hatSql/mz033_dataflow_index_advisor_test.go scripts/benchmark-mz033.sh scripts/benchmark-mz033-baseline.sh scripts/commit-mz033.sh scripts/format-mz032.sh scripts/push-mz033.sh scripts/review-mz033-staged.sh scripts/stage-mz033.sh scripts/test-mz033.sh scripts/verify-mz033.sh
