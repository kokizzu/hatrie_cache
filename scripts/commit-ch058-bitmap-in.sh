#!/usr/bin/env bash
set -euo pipefail

test ! -e hat/hatSql/dataflow_ir.go
test ! -e hat/hatSql/round61_build_prereqs.go
git diff --check
git add \
  BENCHMARK.md \
  CH058_BITMAP_IN_UNION.md \
  ENGINE_IDEAS.md \
  INSPIRATION_BACKLOG.md \
  Makefile \
  hat/hatCache/ch058_bitmap_in_benchmark_test.go \
  hat/hatCache/ch058_bitmap_in_test.go \
  hat/hatCache/sql_query.go \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  scripts/benchmark-ch058-bitmap-in.sh \
  scripts/commit-ch058-bitmap-in.sh \
  scripts/format-ch058-bitmap-in.sh \
  scripts/push-ch058-bitmap-in.sh \
  scripts/test-ch058-bitmap-in.sh \
  scripts/verify-ch058-bitmap-in.sh
git diff --cached --check
git diff --cached --stat
git commit -m 'feat: batch bitmap indexed IN lookups [skip ci]'
