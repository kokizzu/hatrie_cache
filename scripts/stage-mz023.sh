#!/bin/sh
set -eu

git add \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ023_SINK_DELIVERY_AUDIT.md \
  README.md \
  Makefile \
  hat/hatSql/sql_sink_commit.go \
  hat/hatSql/sql_sink_audit_test.go \
  hat/hatSql/sql_sink_delivery_audit.go \
  hat/hatSql/sql_sink_delivery_audit_benchmark_test.go \
  scripts/benchmark-mz023-baseline.sh \
  scripts/benchmark-mz023.sh \
  scripts/format-mz023.sh \
  scripts/test-mz023.sh \
  scripts/verify-mz023.sh \
  scripts/stage-mz023.sh \
  scripts/commit-mz023.sh \
  scripts/push-mz023.sh
git diff --cached --check
git status --short
