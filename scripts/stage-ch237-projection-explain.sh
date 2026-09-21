#!/usr/bin/env bash
set -euo pipefail

git add ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CH237_PROJECTION_EXPLAIN.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  hat/hatCache/sql_query.go \
  hat/hatSql/ch237_projection_explain.go \
  hat/hatSql/ch237_projection_explain_benchmark_test.go \
  hat/hatSql/ch237_projection_explain_test.go \
  hat/hatSql/model.go \
  hat/hatSql/query.go \
  scripts/benchmark-ch237-projection-explain.sh \
  scripts/commit-ch237-projection-explain.sh \
  scripts/format-ch237-projection-explain.sh \
  scripts/race-ch237-projection-explain.sh \
  scripts/stage-ch237-projection-explain.sh \
  scripts/test-ch237-projection-explain.sh \
  scripts/vet-ch237-projection-explain.sh \
  scripts/push-ch237-projection-explain.sh
