#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M219_BACKGROUND_POINT_LOOKUP_BUILD.md \
  hat/hatSql/materialized.go \
  hat/hatSql/materialized_point_lookup.go \
  hat/hatSql/materialized_point_lookup_build.go \
  hat/hatSql/m219_background_point_lookup_test.go \
  hat/hatSql/m219_background_point_lookup_benchmark_test.go \
  scripts/benchmark-m219-background-point-lookup.sh \
  scripts/commit-m219-background-point-lookup.sh \
  scripts/format-m219-background-point-lookup.sh \
  scripts/push-m219-background-point-lookup.sh \
  scripts/race-m219-background-point-lookup.sh \
  scripts/stage-m219-background-point-lookup.sh \
  scripts/test-m219-background-point-lookup.sh \
  scripts/test-m219-related-materialized.sh \
  scripts/vet-m219-background-point-lookup.sh
