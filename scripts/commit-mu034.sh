#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  Makefile \
  M034_HISTORICAL_SUBSCRIPTION_CHECKPOINTS.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/differential_subscription.go \
  hat/hatSql/mu034_historical_subscription_checkpoint.go \
  hat/hatSql/mu034_historical_subscription_checkpoint_benchmark_test.go \
  hat/hatSql/mu034_historical_subscription_checkpoint_test.go \
  hat/hatSql/subscription.go \
  scripts/benchmark-mu034.sh \
  scripts/commit-mu034.sh \
  scripts/format-mu034.sh \
  scripts/push-mu034.sh \
  scripts/race-mu034.sh \
  scripts/test-mu034-package.sh \
  scripts/test-mu034-repo.sh \
  scripts/test-mu034.sh \
  scripts/vet-mu034.sh
git diff --cached --check
git commit -m "hatSql: add historical subscription checkpoints"
