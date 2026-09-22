#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M220_POINT_LOOKUP_RETIREMENT.md \
  hat/hatSql/materialized.go \
  hat/hatSql/materialized_point_lookup.go \
  hat/hatSql/materialized_point_lookup_build.go \
  hat/hatSql/materialized_point_lookup_retirement.go \
  hat/hatSql/m220_point_lookup_retirement_test.go \
  hat/hatSql/m220_point_lookup_retirement_benchmark_test.go \
  scripts/format-m220-point-lookup-retirement.sh \
  scripts/test-m220-point-lookup-retirement.sh \
  scripts/benchmark-m220-point-lookup-retirement.sh \
  scripts/race-m220-point-lookup-retirement.sh \
  scripts/test-m220-related-materialized.sh \
  scripts/vet-m220-point-lookup-retirement.sh \
  scripts/stage-m220-point-lookup-retirement.sh \
  scripts/commit-m220-point-lookup-retirement.sh \
  scripts/push-m220-point-lookup-retirement.sh \
  scripts/status-m220-point-lookup-retirement.sh
