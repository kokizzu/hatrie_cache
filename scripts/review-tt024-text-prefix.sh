#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check
git diff --stat -- BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md README.md SQL_TEXT_PREFIX_INDEX.md Makefile hat/hatCache/monitoring.go hat/hatCache/sql_query.go hat/hatCache/sql_text_prefix.go hat/hatCache/sql_text_prefix_test.go hat/hatCache/sql_text_prefix_benchmark_test.go hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/rewrite.go hat/hatSql/text.go hat/hatSql/text_prefix.go scripts/test-tt024-text-prefix.sh scripts/format-tt024-text-prefix.sh scripts/benchmark-tt024-text-prefix.sh scripts/review-tt024-text-prefix.sh scripts/commit-tt024-text-prefix.sh scripts/push-tt024-text-prefix.sh
rg -n -e 'CONTAINS_PREFIX|SQLTextPrefix|TT-024|SQL_TEXT_PREFIX_INDEX' BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md README.md SQL_TEXT_PREFIX_INDEX.md hat/hatCache hat/hatSql
