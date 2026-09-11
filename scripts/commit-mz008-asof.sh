#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add Makefile README.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md SQL_AS_OF.md sql_as_of_api.go hat/hatSql/query.go hat/hatSql/keyset.go hat/hatSql/sql_as_of.go hat/hatSql/mz008_asof_test.go hat/hatSql/mz008_asof_benchmark_test.go scripts/benchmark-mz008-asof.sh scripts/format-mz008-asof.sh scripts/test-mz008-asof.sh scripts/test-race-mz008-asof.sh scripts/test-mz008-broad.sh scripts/verify-mz008-docs.sh scripts/review-mz008-asof.sh scripts/commit-mz008-asof.sh scripts/push-mz008-asof.sh
git diff --cached --check
git commit -m "Add opt-in SQL AS OF reads"
