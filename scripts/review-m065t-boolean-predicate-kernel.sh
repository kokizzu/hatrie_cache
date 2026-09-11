#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
  Makefile \
  hat/hatSql/columnar_bool_predicate.go \
  hat/hatSql/columnar_bool_predicate_test.go \
  hat/hatSql/columnar_bool_predicate_benchmark_test.go \
  hat/hatSql/query.go \
  README.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  INSPIRATION.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  SQL_PACKED_BOOLEAN_PREDICATE.md \
  scripts/audit-next-engine-idea.sh \
  scripts/test-m065t-boolean-predicate-kernel.sh \
  scripts/benchmark-m065t-boolean-predicate-kernel-baseline.sh \
  scripts/benchmark-m065t-boolean-predicate-kernel.sh \
  scripts/format-m065t-boolean-predicate-kernel.sh \
  scripts/test-m065t-boolean-predicate-kernel-full.sh \
  scripts/race-m065t-boolean-predicate-kernel.sh \
  scripts/vet-m065t-boolean-predicate-kernel.sh \
  scripts/verify-m065t-boolean-predicate-kernel-docs.sh \
  scripts/review-m065t-boolean-predicate-kernel.sh \
  scripts/commit-m065t-boolean-predicate-kernel.sh \
  scripts/push-m065t-boolean-predicate-kernel.sh
git diff --stat -- \
  Makefile \
  hat/hatSql/columnar_bool_predicate.go \
  hat/hatSql/columnar_bool_predicate_test.go \
  hat/hatSql/columnar_bool_predicate_benchmark_test.go \
  hat/hatSql/query.go \
  README.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  INSPIRATION.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  SQL_PACKED_BOOLEAN_PREDICATE.md \
  scripts/audit-next-engine-idea.sh \
  scripts/test-m065t-boolean-predicate-kernel.sh \
  scripts/benchmark-m065t-boolean-predicate-kernel-baseline.sh \
  scripts/benchmark-m065t-boolean-predicate-kernel.sh \
  scripts/format-m065t-boolean-predicate-kernel.sh \
  scripts/test-m065t-boolean-predicate-kernel-full.sh \
  scripts/race-m065t-boolean-predicate-kernel.sh \
  scripts/vet-m065t-boolean-predicate-kernel.sh \
  scripts/verify-m065t-boolean-predicate-kernel-docs.sh \
  scripts/review-m065t-boolean-predicate-kernel.sh \
  scripts/commit-m065t-boolean-predicate-kernel.sh \
  scripts/push-m065t-boolean-predicate-kernel.sh
git status --short --untracked-files=all
