#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C250_ASYNC_INSERT_IDENTITIES.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  scripts/inspect-c250-scope.sh \
  scripts/review-c250-async-dedup.sh \
  scripts/stage-c250-async-dedup.sh \
  scripts/commit-c250-async-dedup.sh \
  scripts/push-c250-async-dedup.sh
git diff --cached --check
git diff --cached --stat
