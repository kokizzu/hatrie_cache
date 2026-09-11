#!/bin/sh
set -eu

git add \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  TT040_SPACE_CHANGEFEED.md \
  hat/hatCache/journal_segments.go \
  hat/hatCache/journal_space_subscription_benchmark_test.go \
  hat/hatCache/journal_space_subscription_test.go \
  hat/hatCache/journal_subscription.go \
  scripts/benchmark-tt040-space-changefeed.sh \
  scripts/commit-tt040-space-changefeed.sh \
  scripts/format-tt040-space-changefeed.sh \
  scripts/inspect-engine-ideas.sh \
  scripts/push-tt040-space-changefeed.sh \
  scripts/review-tt040-space-changefeed.sh \
  scripts/test-race-tt040-space-changefeed.sh \
  scripts/test-tt040-space-changefeed.sh \
  scripts/verify-tt040-docs.sh \
  scripts/vet-tt040-space-changefeed.sh
git commit -m "chore: add TT-040 workflow scripts"
