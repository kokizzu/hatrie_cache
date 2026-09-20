#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  MU031_RETRACTABLE_AGGREGATES.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/aggregate_combinator.go \
  hat/hatSql/mu031_transactional_aggregate.go \
  hat/hatSql/mu031_transaction_test.go \
  hat/hatSql/mu031_transaction_benchmark_test.go \
  hat/hatSql/mu031_transaction_rollback_test.go \
  scripts/benchmark-mu031-transactional-aggregate.sh \
  scripts/review-mu031-transactional-aggregate.sh \
  scripts/stage-mu031-transactional-aggregate.sh \
  scripts/commit-mu031-transactional-aggregate.sh \
  scripts/push-mu031-transactional-aggregate.sh
git diff --cached --check
git diff --cached --stat
