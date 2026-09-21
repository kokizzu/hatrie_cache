#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M215_DELTA_JOIN_AUDIT.md \
  scripts/benchmark-m215-delta-join-audit.sh \
  scripts/commit-m215-delta-join-audit.sh \
  scripts/push-m215-delta-join-audit.sh \
  scripts/stage-m215-delta-join-audit.sh \
  scripts/test-m215-delta-join-audit.sh
