#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  Makefile \
  PARTITION_OWNERSHIP_CONSENSUS.md \
  hat/hatTopology/ownership_consensus_benchmark_test.go \
  hat/hatTopology/ownership_consensus_collector.go \
  hat/hatTopology/ownership_consensus_collector_test.go \
  scripts/benchmark-c153-collector.sh \
  scripts/commit-c153-collector.sh \
  scripts/format-c153-collector.sh \
  scripts/push-c153-collector.sh \
  scripts/race-c153-collector.sh \
  scripts/review-c153-collector.sh \
  scripts/test-c153-collector.sh \
  scripts/vet-c153-collector.sh
git diff --cached --check
git commit -m 'feat(topology): add reusable ownership quorum collector [skip ci]'
