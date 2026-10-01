#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU13_DURABLE_CLUSTER_MEMBERSHIP.md \
  hat/hatTopology/tu13_membership.go \
  hat/hatTopology/tu13_membership_benchmark_test.go \
  hat/hatTopology/tu13_membership_test.go \
  scripts/benchmark-tu13-membership.sh \
  scripts/commit-tu13-membership.sh \
  scripts/format-tu13-membership.sh \
  scripts/push-tu13-membership.sh \
  scripts/test-tu13-membership.sh \
  scripts/verify-tu13-membership.sh

git diff --cached --check
git commit -m 'feat: add durable cluster membership log [skip ci]'
