#!/usr/bin/env bash
set -euo pipefail
git add \
  BENCHMARK.md \
  INSPIRATION.md \
  Makefile \
  T047_CLUSTER_WRITE_COMMIT.md \
  hat/hatReplication/t047_quorum_ledger.go \
  hat/hatReplication/t047_quorum_ledger_benchmark_test.go \
  hat/hatReplication/t047_quorum_ledger_test.go \
  scripts/format-t047-quorum-ledger.sh \
  scripts/race-t047-quorum-ledger.sh \
  scripts/commit-t047-quorum-ledger.sh \
  scripts/push-t047-quorum-ledger.sh \
  scripts/stage-t047-quorum-ledger.sh \
  scripts/status-t047-quorum-ledger.sh \
  scripts/test-t047-quorum-ledger.sh \
  scripts/vet-t047-quorum-ledger.sh
git diff --cached --check
