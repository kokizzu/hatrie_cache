#!/usr/bin/env bash
set -euo pipefail

paths=(
  M30_DIFFERENTIAL_SUBSCRIPTIONS.md
  hat/hatSql/differential_subscription.go
  hat/hatSql/differential_subscription_benchmark_test.go
  hat/hatSql/differential_subscription_public_test.go
  hat/hatSql/differential_subscription_test.go
  hat/hatSql/subscription.go
  scripts/benchmark-m-g30-differential-subscription.sh
  scripts/commit-m-g30-differential-subscription.sh
  scripts/format-m-g30-differential-subscription.sh
  scripts/inspect-m-g30-context.sh
  scripts/publish-m-g30-differential-subscription.sh
  scripts/race-m-g30-differential-subscription.sh
  scripts/test-m-g30-differential-subscription.sh
  scripts/test-m-g30-package.sh
  scripts/vet-m-g30-differential-subscription.sh
)

git add -- "${paths[@]}"
git diff --cached --check -- "${paths[@]}"
git commit --only -m 'feat(sql): add differential subscriptions' -- "${paths[@]}"
