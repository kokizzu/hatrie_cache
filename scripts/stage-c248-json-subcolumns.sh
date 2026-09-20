#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C248_VARIANT_JSON_SUBCOLUMNS.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  scripts/inspect-c248-scope.sh \
  scripts/review-c248-json-subcolumns.sh \
  scripts/stage-c248-json-subcolumns.sh \
  scripts/commit-c248-json-subcolumns.sh \
  scripts/push-c248-json-subcolumns.sh
git diff --cached --check
git diff --cached --stat
