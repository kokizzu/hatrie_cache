#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M218_MATERIALIZED_POINT_LOOKUP_PLANNER.md \
  hat/hatSql/materialized_point_lookup_resolver.go \
  hat/hatSql/materialized_point_lookup_resolver_test.go \
  hat/hatSql/materialized_point_lookup_resolver_benchmark_test.go \
  scripts/format-m218-point-lookup-planner.sh \
  scripts/test-m218-point-lookup-planner.sh \
  scripts/benchmark-m218-point-lookup-planner.sh \
  scripts/race-m218-point-lookup-planner.sh \
  scripts/stage-m218-point-lookup-planner.sh \
  scripts/commit-m218-point-lookup-planner.sh \
  scripts/push-m218-point-lookup-planner.sh
