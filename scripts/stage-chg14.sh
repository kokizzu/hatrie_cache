#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  BENCHMARK.md \
  PRODUCT_IDEA_GAPS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION.md \
  TU13_DURABLE_MEMBERSHIP.md \
  hat/hatTopology/durable_membership.go \
  hat/hatTopology/durable_membership_test.go \
  hat/hatTopology/durable_membership_benchmark_test.go \
  scripts/benchmark-chg14.sh \
  scripts/check-chg14.sh \
  scripts/format-chg14.sh \
  scripts/race-chg14.sh \
  scripts/status-chg14.sh \
  scripts/stage-chg14.sh \
  scripts/commit-chg14.sh \
  scripts/push-chg14.sh \
  scripts/test-chg14.sh \
  scripts/test-full-chg14.sh \
  scripts/vet-chg14.sh
