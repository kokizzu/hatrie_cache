#!/usr/bin/env bash
set -euo pipefail

git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md ENGINE_IDEAS.md README.md SQL_GROUPING_IDENTIFIERS.md Makefile hat/hatSql/grouping_sets.go hat/hatSql/query.go hat/hatSql/grouping_identifier_test.go hat/hatSql/grouping_identifier_benchmark_test.go scripts/benchmark-ch041-baseline.sh scripts/benchmark-ch041-grouping-identifiers.sh scripts/format-ch041-grouping-identifiers.sh scripts/test-ch041-grouping-identifiers.sh scripts/review-ch041-grouping-identifiers.sh scripts/commit-ch041-grouping-identifiers.sh scripts/push-ch041-grouping-identifiers.sh
git commit -m "sql: add grouping set identifiers"
