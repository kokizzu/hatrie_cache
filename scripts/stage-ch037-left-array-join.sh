#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  CH037_LEFT_ARRAY_JOIN.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  hat/hatSql/array_join_test.go \
  hat/hatSql/ch037_left_array_join_benchmark_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-ch037-left-array-join-before.sh \
  scripts/benchmark-ch037-left-array-join.sh \
  scripts/commit-ch037-left-array-join.sh \
  scripts/format-ch037-left-array-join.sh \
  scripts/push-ch037-left-array-join.sh \
  scripts/race-ch037-left-array-join.sh \
  scripts/stage-ch037-left-array-join.sh \
  scripts/test-ch037-left-array-join.sh \
  scripts/test-ch037-sql-package.sh \
  scripts/vet-ch037-left-array-join.sh
git diff --cached --check
git status --short
