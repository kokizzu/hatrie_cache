#!/usr/bin/env bash
set -euo pipefail

git add BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md README.md SQL_TEXT_PREFIX_INDEX.md Makefile hat/hatCache/monitoring.go hat/hatCache/sql_query.go hat/hatCache/sql_text_prefix.go hat/hatCache/sql_text_prefix_test.go hat/hatCache/sql_text_prefix_benchmark_test.go hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/rewrite.go hat/hatSql/text.go hat/hatSql/text_prefix.go scripts/test-tt024-text-prefix.sh scripts/format-tt024-text-prefix.sh scripts/benchmark-tt024-text-prefix.sh scripts/review-tt024-text-prefix.sh scripts/commit-tt024-text-prefix.sh scripts/push-tt024-text-prefix.sh
git commit -m "sql: add text prefix index lookup"
