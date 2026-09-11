#!/usr/bin/env bash
set -euo pipefail

git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md ENGINE_IDEAS.md README.md SQL_AGGREGATE_IF.md Makefile hat/hatSql/aggregate_if.go hat/hatSql/aggregate_if_test.go hat/hatSql/aggregate_if_benchmark_test.go hat/hatSql/query.go scripts/test-ch038-aggregate-if.sh scripts/format-ch038-aggregate-if.sh scripts/benchmark-ch038-aggregate-if.sh scripts/review-ch038-aggregate-if.sh scripts/commit-ch038-aggregate-if.sh scripts/push-ch038-aggregate-if.sh
git commit -m "sql: add aggregate If combinators"
