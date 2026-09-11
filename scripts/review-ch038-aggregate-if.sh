#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check
git diff --stat -- ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md ENGINE_IDEAS.md README.md SQL_AGGREGATE_IF.md Makefile hat/hatSql/aggregate_if.go hat/hatSql/aggregate_if_test.go hat/hatSql/aggregate_if_benchmark_test.go hat/hatSql/query.go scripts/test-ch038-aggregate-if.sh scripts/format-ch038-aggregate-if.sh scripts/benchmark-ch038-aggregate-if.sh scripts/review-ch038-aggregate-if.sh scripts/commit-ch038-aggregate-if.sh scripts/push-ch038-aggregate-if.sh
rg -n -e 'COUNT_IF|COUNTIF|SUM_IF|SUMIF|ARGMAX_IF|ARGMAXIF|CH-038|SQL_AGGREGATE_IF' ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md ENGINE_IDEAS.md README.md SQL_AGGREGATE_IF.md hat/hatSql/aggregate_if.go hat/hatSql/aggregate_if_test.go hat/hatSql/aggregate_if_benchmark_test.go hat/hatSql/query.go scripts/test-ch038-aggregate-if.sh scripts/format-ch038-aggregate-if.sh scripts/benchmark-ch038-aggregate-if.sh scripts/review-ch038-aggregate-if.sh scripts/commit-ch038-aggregate-if.sh scripts/push-ch038-aggregate-if.sh
