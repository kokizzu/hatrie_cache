#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  SQL_PREWHERE.md \
  hat/hatSql/ch001_prewhere_benchmark_test.go \
  hat/hatSql/ch001_prewhere_test.go \
  hat/hatSql/collation.go \
  hat/hatSql/index_advisor.go \
  hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/prewhere.go \
  hat/hatSql/query.go \
  hat/hatSql/rewrite.go \
  hat/hatSql/subquery.go \
  hat/hatSql/whatif.go \
  scripts/benchmark-ch001-prewhere.sh \
  scripts/commit-ch001-prewhere.sh \
  scripts/format-ch001-prewhere.sh \
  scripts/inspect-sql-prewhere.sh \
  scripts/push-ch001-prewhere.sh \
  scripts/review-ch001-prewhere.sh \
  scripts/test-ch001-prewhere.sh \
  scripts/test-race-ch001-prewhere.sh \
  scripts/vet-ch001-prewhere.sh
git diff --cached --check
git diff --cached --stat
git commit -m "hatSql: add explicit PREWHERE stage"
