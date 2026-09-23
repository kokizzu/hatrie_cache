#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  README.md \
  T209_RELAY_APPLIER_BACKPRESSURE.md \
  Makefile \
  hat/hatCache/replication.go \
  hat/hatCache/t209_relay_backpressure_benchmark_test.go \
  hat/hatCache/t209_relay_backpressure_test.go \
  hat/hatReplication/model.go \
  hat/hatReplication/relay_backpressure.go \
  hat/hatReplication/t209_relay_backpressure_benchmark_test.go \
  hat/hatReplication/t209_relay_backpressure_test.go \
  scripts/benchmark-t209-before.sh \
  scripts/benchmark-t209.sh \
  scripts/commit-t209.sh \
  scripts/format-t209.sh \
  scripts/race-t209.sh \
  scripts/push-t209.sh \
  scripts/stage-t209.sh \
  scripts/test-t209-package.sh \
  scripts/test-t209.sh \
  scripts/verify-t209-scope.sh \
  scripts/vet-t209.sh
