#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  MZ010_SQL_SUBSCRIPTIONS.md \
  Makefile \
  hat/hatSql/mz010_subscription_envelope.go \
  hat/hatSql/mz010_subscription_envelope_benchmark_test.go \
  hat/hatSql/mz010_subscription_envelope_test.go \
  scripts/benchmark-mz010-subscription-envelope.sh \
  scripts/commit-mz010-subscription-envelope.sh \
  scripts/format-mz010-subscription-envelope.sh \
  scripts/push-mz010-subscription-envelope.sh \
  scripts/race-mz010-subscription-envelope.sh \
  scripts/review-mz010-subscription-envelope.sh \
  scripts/test-mz010-subscription-envelope.sh \
  scripts/test-mz010-subscription-envelope-package.sh \
  scripts/vet-mz010-subscription-envelope.sh
git diff --cached --check
git commit -m "feat: add signed SQL subscription envelopes"
