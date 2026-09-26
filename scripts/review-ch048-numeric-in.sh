#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat -- \
  BENCHMARK.md \
  CH048_NUMERIC_IN.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  ENGINE_IDEAS.md \
  Makefile \
  hat/hatSql/columnar_numeric_in_predicate.go \
  hat/hatSql/ch048_numeric_in_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-ch048-numeric-in.sh \
  scripts/commit-ch048-numeric-in.sh \
  scripts/format-ch048-numeric-in.sh \
  scripts/push-ch048-numeric-in.sh \
  scripts/race-ch048-numeric-in.sh \
  scripts/review-ch048-numeric-in.sh \
  scripts/stage-ch048-numeric-in.sh \
  scripts/test-ch048-numeric-in-package.sh \
  scripts/test-ch048-numeric-in.sh \
  scripts/verify-ch048-numeric-in-docs.sh \
  scripts/vet-ch048-numeric-in.sh
