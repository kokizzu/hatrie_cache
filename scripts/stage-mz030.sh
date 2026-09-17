#!/bin/sh
set -eu

git add \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ030_LOOKUP_JOIN_CACHE.md \
  README.md \
  Makefile \
  hat/hatSql/query.go \
  hat/hatSql/mz030_lookup_join_cache.go \
  hat/hatSql/mz030_lookup_join_cache_test.go \
  hat/hatSql/mz030_lookup_cache_benchmark_test.go \
  scripts/benchmark-mz030-baseline.sh \
  scripts/benchmark-mz030.sh \
  scripts/format-mz030.sh \
  scripts/test-mz030.sh \
  scripts/verify-mz030.sh \
  scripts/stage-mz030.sh \
  scripts/commit-mz030.sh \
  scripts/push-mz030.sh
git diff --cached --check
git status --short
