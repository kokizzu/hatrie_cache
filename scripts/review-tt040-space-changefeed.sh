#!/bin/sh
set -eu

git diff --stat -- \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  TT040_SPACE_CHANGEFEED.md \
  hat/hatCache/journal_segments.go \
  hat/hatCache/journal_space_subscription_benchmark_test.go \
  hat/hatCache/journal_space_subscription_test.go \
  hat/hatCache/journal_subscription.go \
  scripts
git diff --check
git status --short
