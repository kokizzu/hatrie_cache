#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  MZ033_TIMESTAMP_ORACLE.md \
  README.md \
  scripts/benchmark-mz033-timestamp-oracle.sh \
  scripts/commit-mz033-timestamp-oracle.sh \
  scripts/format-mz033-timestamp-oracle.sh \
  scripts/inspect-mz033-c203.sh \
  scripts/push-mz033-timestamp-oracle.sh \
  scripts/race-mz033-timestamp-oracle.sh \
  scripts/review-mz033-timestamp-oracle.sh \
  scripts/test-mz033-timestamp-oracle.sh \
  scripts/verify-mz033-timestamp-oracle.sh \
  scripts/vet-mz033-timestamp-oracle.sh
printf 'n\nn\ny\n' | git add -p -- Makefile
git diff --cached --check
git commit -m 'docs: record existing global timestamp oracle'
