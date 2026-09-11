#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  SQL_ORDERED_RANGE_PRUNING.md \
  hat/hatCache/ch002_primary_mark_pruning_benchmark_test.go \
  hat/hatCache/ch002_primary_mark_pruning_test.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/sql_query.go \
  hat/hatSql/ch002_primary_mark_pruning_benchmark_test.go \
  hat/hatSql/ch002_primary_mark_pruning_test.go \
  hat/hatSql/contracts.go \
  hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/ordered_range.go \
  hat/hatSql/query.go \
  hat/hatSql/session.go \
  scripts/benchmark-ch002-hattrie-materialized.sh \
  scripts/benchmark-ch002-hattrie-stream.sh \
  scripts/benchmark-ch002-hattrie.sh \
  scripts/benchmark-ch002-primary-mark-pruning.sh \
  scripts/benchmark-ch002-stream-sparse.sh \
  scripts/commit-ch002.sh \
  scripts/format-ch002-primary-mark-pruning.sh \
  scripts/list-engine-ideas.sh \
  scripts/review-ch002-code.sh \
  scripts/review-ch002.sh \
  scripts/test-ch002-primary-mark-pruning.sh \
  scripts/test-ch002-real-index.sh \
  scripts/test-race-ch002-primary-mark-pruning.sh \
  scripts/verify-ch002-docs.sh \
  scripts/vet-ch002-primary-mark-pruning.sh \
  scripts/push-ch002.sh
git commit -m "hatSql: add ordered range pruning"
