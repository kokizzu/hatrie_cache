#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU06_REPLICA_READ_ONLY_GATE.md \
  Makefile \
  hat/hatReplication/read_only_gate.go \
  hat/hatReplication/read_only_gate_benchmark_test.go \
  hat/hatReplication/read_only_gate_test.go \
  scripts/benchmark-round36-readonly.sh \
  scripts/commit-round36-readonly.sh \
  scripts/format-round36-readonly.sh \
  scripts/push-round36-readonly.sh \
  scripts/race-round36-readonly.sh \
  scripts/review-round36-readonly.sh \
  scripts/stage-round36-readonly.sh \
  scripts/test-round36-readonly.sh \
  scripts/verify-round36-readonly-docs.sh \
  scripts/vet-round36-readonly.sh
