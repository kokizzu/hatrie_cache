#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C226_GRACE_HASH_JOIN.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  scripts/benchmark-c226-grace-hash-join.sh \
  scripts/commit-c226-grace-hash-join.sh \
  scripts/push-c226-grace-hash-join.sh \
  scripts/race-c226-grace-hash-join.sh \
  scripts/test-c226-grace-hash-join.sh
git commit -m "docs: verify grace-hash join spilling"
