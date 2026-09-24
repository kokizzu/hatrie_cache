#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add \
  BENCHMARK.md \
  CH037_LEFT_ARRAY_JOIN.md \
  ENGINE_IDEAS.md \
  Makefile \
  hat/hatSql/array_join.go \
  hat/hatSql/ch037_nested_array_join_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-ch037-array-join.sh \
  scripts/commit-ch037-nested-array-join.sh \
  scripts/format-ch037-nested-array-join.sh \
  scripts/push-ch037-nested-array-join.sh \
  scripts/race-ch037-nested-array-join.sh \
  scripts/review-ch037-nested-array-join.sh \
  scripts/test-ch037-nested-array-join-package.sh \
  scripts/test-ch037-nested-array-join.sh \
  scripts/vet-ch037-nested-array-join.sh
git diff --cached --check
git commit -m "feat: support nested SQL array joins"
