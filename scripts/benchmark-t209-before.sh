#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  bash scripts/cleanup-go-build-tmp-safe.sh plan >/dev/null 2>&1 || true
  bash scripts/cleanup-go-build-tmp-safe.sh apply >/dev/null 2>&1 || true
  bash scripts/cleanup-go-build-tmp-safe.sh clean-metadata >/dev/null 2>&1 || true
}
trap cleanup EXIT

go test ./hat/hatCache \
  -run '^$' \
  -bench '^BenchmarkTR050ReplicationByteBudgetAdmission$' \
  -benchtime=300ms \
  -count=5
