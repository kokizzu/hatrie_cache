#!/bin/sh
set -eu

git add BENCHMARK.md INSPIRATION.md Makefile \
  scripts/review-replay-rollback.sh \
  scripts/commit-replay-rollback.sh \
  scripts/push-replay-rollback.sh
git diff --cached --check
git commit -m "docs: record rejected parallel replay"
