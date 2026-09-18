#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CH009_TTL_ROLLUP.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  hat/hatSql/ch008_column_ttl_test.go \
  hat/hatSql/ch009_ttl_rollup_benchmark_test.go \
  hat/hatSql/ch009_ttl_rollup_test.go \
  hat/hatSql/typed_table_ttl_rollup.go \
  hat/hatSql/typed_table_ttl_scheduler.go \
  scripts/benchmark-ch009-ttl-rollup.sh \
  scripts/commit-ch009-ttl-rollup.sh \
  scripts/format-ch009-ttl-rollup.sh \
  scripts/push-ch009-ttl-rollup.sh \
  scripts/review-ch009.sh \
  scripts/stage-ch009-ttl-rollup.sh \
  scripts/test-ch009-ttl-rollup.sh
