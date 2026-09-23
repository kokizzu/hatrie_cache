#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M226_DURABLE_CONSENSUS_METADATA.md \
  Makefile \
  README.md \
  hat/hatSql/m226_consensus_metadata.go \
  hat/hatSql/m226_consensus_metadata_benchmark_test.go \
  hat/hatSql/m226_consensus_metadata_test.go \
  scripts/m226-benchmark.sh \
  scripts/m226-commit.sh \
  scripts/m226-format.sh \
  scripts/m226-push.sh \
  scripts/m226-race.sh \
  scripts/m226-stage.sh \
  scripts/m226-test.sh \
  scripts/m226-vet.sh
git diff --cached --check
git diff --cached --stat
