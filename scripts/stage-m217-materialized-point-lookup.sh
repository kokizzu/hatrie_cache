#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M217_MATERIALIZED_POINT_LOOKUP.md \
  hat/hatSql/materialized.go \
  hat/hatSql/materialized_point_lookup.go \
  hat/hatSql/materialized_point_lookup_test.go \
  hat/hatSql/materialized_point_lookup_benchmark_test.go \
  scripts/benchmark-m217-point-lookup.sh \
  scripts/commit-m217-materialized-point-lookup.sh \
  scripts/format-m217-point-lookup.sh \
  scripts/push-m217-materialized-point-lookup.sh \
  scripts/race-m217-point-lookup.sh \
  scripts/stage-m217-materialized-point-lookup.sh \
  scripts/test-m217-materialized-point-lookup.sh
