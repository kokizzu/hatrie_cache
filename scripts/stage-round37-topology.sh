#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU13_DURABLE_MEMBERSHIP.md \
  Makefile \
  hat/hatTopology/durable_membership.go \
  hat/hatTopology/tu13_durable_membership_baseline_benchmark_test.go \
  hat/hatTopology/tu13_durable_membership_test.go \
  scripts/commit-round37-topology.sh \
  scripts/format-round37-topology.sh \
  scripts/push-round37-topology.sh \
  scripts/review-round37-topology.sh \
  scripts/stage-round37-topology.sh \
  scripts/test-round37-topology.sh \
  scripts/verify-round37-topology-docs.sh
