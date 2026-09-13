#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  TR015_PERSISTENT_FILTER_TELEMETRY.md \
  Makefile \
  hat/hatCache/pebble_store.go \
  hat/hatCache/tr015_persistent_store_metrics_benchmark_test.go \
  hat/hatCache/tr015_persistent_store_metrics_test.go \
  hat/hatStorage/capabilities.go \
  scripts/benchmark-tr015.sh \
  scripts/commit-tr015.sh \
  scripts/format-tr015.sh \
  scripts/test-tr015.sh \
  scripts/push-tr015.sh
git diff --cached --check
git commit -m 'feat: expose persistent filter telemetry'
