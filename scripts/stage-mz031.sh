#!/bin/sh
set -eu

git add \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ031_RANKED_TOP_K.md \
  README.md \
  Makefile \
  hat/hatSql/c213_incremental_top_k.go \
  hat/hatSql/mz031_rank_changes_test.go \
  hat/hatSql/mz031_rank_changes_benchmark_test.go \
  hat/hatSql/mz031_rank_changes_incremental_benchmark_test.go \
  scripts/benchmark-mz031-baseline.sh \
  scripts/benchmark-mz031.sh \
  scripts/commit-mz031.sh \
  scripts/format-mz031.sh \
  scripts/push-mz031.sh \
  scripts/stage-mz031.sh \
  scripts/test-mz031.sh \
  scripts/verify-mz031.sh
git diff --cached --check
git status --short
git diff --cached --stat
