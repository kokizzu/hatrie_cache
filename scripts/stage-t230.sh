#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  TT030_ON_REPLACE_CHANGEFEED.md \
  hat/hatDataStructure/memtx_table.go \
  hat/hatDataStructure/t230_on_replace_test.go \
  scripts/benchmark-t230.sh \
  scripts/commit-t230.sh \
  scripts/format-t230.sh \
  scripts/push-t230.sh \
  scripts/race-t230.sh \
  scripts/stage-t230.sh \
  scripts/test-t230.sh \
  scripts/verify-t230-docs.sh \
  scripts/vet-t230.sh

git diff --cached --check
git diff --cached --name-status
