#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M221_CLUSTER_COMPUTE_ISOLATION_AUDIT.md \
  hat/hatSql/m221_cluster_isolation_test.go \
  scripts/format-m221-cluster-isolation.sh \
  scripts/stage-m221-cluster-isolation.sh \
  scripts/commit-m221-cluster-isolation.sh \
  scripts/push-m221-cluster-isolation.sh \
  scripts/status-m221-cluster-isolation.sh
