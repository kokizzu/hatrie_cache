#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  CH021_TRANSPARENT_TIER_READS.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  hat/hatStorage/ch021_tier_reader_benchmark_test.go \
  hat/hatStorage/ch021_tier_reader_test.go \
  hat/hatStorage/storage_tier_reader.go \
  scripts/run-ch021-tier-reader-checks.sh \
  scripts/stage-round18-ch021-tier-reader.sh \
  scripts/commit-round18-ch021-tier-reader.sh \
  scripts/push-round18-ch021-tier-reader.sh

git diff --cached --check
git status --short
