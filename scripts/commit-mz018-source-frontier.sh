#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ018_SOURCE_FRONTIER_WAIT.md \
  Makefile \
  README.md \
  hat/hatSql/keyset.go \
  hat/hatSql/mz018_source_frontier_wait_benchmark_test.go \
  hat/hatSql/mz018_source_frontier_wait_test.go \
  hat/hatSql/query.go \
  hat/hatSql/sql_source_frontier_requirement.go \
  scripts/benchmark-mz018-source-frontier.sh \
  scripts/commit-mz018-source-frontier.sh \
  scripts/format-mz018-source-frontier.sh \
  scripts/race-mz018-source-frontier.sh \
  scripts/test-mz018-source-frontier.sh \
  scripts/vet-mz018-source-frontier.sh
git commit -m "feat: add SQL source frontier waiting"
