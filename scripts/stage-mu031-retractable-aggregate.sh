#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  README.md \
  PRODUCT_IDEA_GAPS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  MU031_RETRACTABLE_AGGREGATES.md \
  hat/hatSql/aggregate_combinator.go \
  hat/hatSql/mu031_retractable_aggregate_test.go \
  hat/hatSql/mu031_retractable_aggregate_benchmark_test.go \
  scripts/benchmark-mu031-retractable-aggregate-baseline.sh \
  scripts/benchmark-mu031-retractable-aggregate.sh \
  scripts/commit-mu031-retractable-aggregate.sh \
  scripts/format-mu031-retractable-aggregate.sh \
  scripts/push-mu031-retractable-aggregate.sh \
  scripts/race-mu031-retractable-aggregate.sh \
  scripts/review-mu031-retractable-aggregate.sh \
  scripts/stage-mu031-retractable-aggregate.sh \
  scripts/test-mu031-package.sh \
  scripts/test-mu031-retractable-aggregate.sh \
  scripts/verify-mu031-retractable-aggregate.sh \
  scripts/vet-mu031-retractable-aggregate.sh
git diff --cached --check
