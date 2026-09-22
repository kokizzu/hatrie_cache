#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION_ROUND2.md \
  BENCHMARK.md \
  T201_PER_SPACE_SYNC_QUORUM.md \
  hat/hatReplication/t201_per_space_write_quorum.go \
  hat/hatReplication/t201_per_space_write_quorum_test.go \
  hat/hatReplication/t201_per_space_write_quorum_benchmark_test.go \
  scripts/test-t201.sh \
  scripts/benchmark-t201.sh \
  scripts/format-t201.sh \
  scripts/race-t201.sh \
  scripts/vet-t201.sh \
  scripts/test-t201-package.sh \
  scripts/verify-docs-t201.sh \
  scripts/stage-t201.sh \
  scripts/commit-t201.sh \
  scripts/push-t201.sh
git diff --cached --stat
git diff --cached --check
git status --short
