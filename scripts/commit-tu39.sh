#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU39_SPACE_CHANGEFEED.md \
  hat/hatReplication/tu39_space_changefeed.go \
  hat/hatReplication/tu39_space_changefeed_test.go \
  hat/hatReplication/tu39_space_changefeed_benchmark_test.go \
  hat/hatReplication/tu39_space_changefeed_baseline_benchmark_test.go \
  scripts/benchmark-tu39.sh \
  scripts/benchmark-tu39-baseline.sh \
  scripts/commit-tu39.sh \
  scripts/format-tu39.sh \
  scripts/race-tu39.sh \
  scripts/test-tu39-package.sh \
  scripts/test-tu39.sh \
  scripts/vet-tu39.sh \
  Makefile
git diff --cached --check
git commit -m "feat(replication): add bounded space changefeed"
