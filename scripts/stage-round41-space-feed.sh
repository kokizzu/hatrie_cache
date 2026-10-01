#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU39_SPACE_CHANGEFEED.md \
  hat/hatCache/journal_space_feed.go \
  hat/hatCache/journal_space_feed_test.go \
  hat/hatCache/journal_space_feed_benchmark_test.go \
  scripts/benchmark-round41-space-feed.sh \
  scripts/check-round41-space-feed.sh \
  scripts/commit-round41-space-feed.sh \
  scripts/format-round41-space-feed.sh \
  scripts/push-round41-space-feed.sh \
  scripts/race-round41-space-feed.sh \
  scripts/stage-round41-space-feed.sh \
  scripts/status-round41-space-feed.sh \
  scripts/test-round41-space-feed.sh \
  scripts/verify-round41-space-feed-docs.sh \
  scripts/vet-round41-space-feed.sh
git diff --cached --check
git diff --cached --stat
