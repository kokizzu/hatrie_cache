#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  Makefile \
  MU034_HISTORICAL_SUBSCRIPTION_CANCELLATION.md \
  PRODUCT_IDEA_GAPS.md \
  hat/hatCache/journal_historical_subscription.go \
  hat/hatCache/journal_source_checkpoint.go \
  hat/hatCache/m_u34_historical_subscription_benchmark_test.go \
  hat/hatCache/m_u34_historical_subscription_test.go \
  scripts/benchmark-mu34-baseline.sh \
  scripts/benchmark-mu34.sh \
  scripts/commit-mu34.sh \
  scripts/format-mu34.sh \
  scripts/push-mu34.sh \
  scripts/race-mu34.sh \
  scripts/review-mu34.sh \
  scripts/stage-mu34.sh \
  scripts/test-mu34.sh \
  scripts/vet-mu34.sh

git diff --cached --check
git status --short
