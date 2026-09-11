#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION.md \
  Makefile \
  PARTITION_OWNERSHIP.md \
  PARTITION_OWNERSHIP_CONSENSUS.md \
  hat/hatTopology/ownership_consensus.go \
  hat/hatTopology/ownership_consensus_benchmark_test.go \
  hat/hatTopology/ownership_consensus_test.go \
  scripts/benchmark-partition-ownership-consensus.sh \
  scripts/commit-partition-ownership-consensus.sh \
  scripts/format-partition-ownership-consensus.sh \
  scripts/test-partition-ownership-consensus.sh \
  scripts/verify-partition-ownership-consensus.sh
git diff --cached --check
git diff --cached --stat
git commit -m "hatTopology: remove temporary inspection target"
git push origin HEAD:master
