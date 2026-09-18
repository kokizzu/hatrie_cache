#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CH008_COLUMN_TTL.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  hat/hatSql/ch008_column_ttl_benchmark_test.go \
  hat/hatSql/ch008_column_ttl_test.go \
  hat/hatSql/typed_table.go \
  hat/hatSql/typed_table_column_ttl_snapshot.go \
  hat/hatSql/typed_table_histogram.go \
  hat/hatSql/typed_table_patch_parts.go \
  hat/hatSql/typed_table_stats.go \
  hat/hatSql/typed_table_ttl.go \
  hat/hatSql/typed_table_ttl_scheduler.go \
  scripts/benchmark-ch008-column-ttl.sh \
  scripts/format-ch008-column-ttl-snapshot.sh \
  scripts/format-ch008-column-ttl.sh \
  scripts/format-ch008-scheduler.sh \
  scripts/review-ch008.sh \
  scripts/stage-ch008-column-ttl.sh \
  scripts/commit-ch008-column-ttl.sh \
  scripts/push-ch008-column-ttl.sh \
  scripts/test-ch008-column-ttl.sh

git diff --cached --check
git status --short
