#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C213_TYPED_HASH_JOIN.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  INSPIRATION.md \
  Makefile \
  hat/hatSql/c213_typed_hash_join_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-c213-typed-hash-join.sh \
  scripts/commit-c213-typed-hash-join.sh \
  scripts/format-c213-typed-hash-join.sh \
  scripts/push-c213-typed-hash-join.sh \
  scripts/race-c213-typed-hash-join.sh \
  scripts/report-open-inspiration.sh \
  scripts/stage-c213-typed-hash-join.sh \
  scripts/test-c213-typed-hash-join.sh \
  scripts/verify-c213-typed-hash-join.sh \
  scripts/vet-c213-typed-hash-join.sh

git diff --cached --check
git status --short
