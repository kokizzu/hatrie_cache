#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  bash scripts/cleanup-go-build-tmp-safe.sh plan >/dev/null 2>&1 || true
  bash scripts/cleanup-go-build-tmp-safe.sh apply >/dev/null 2>&1 || true
  bash scripts/cleanup-go-build-tmp-safe.sh clean-metadata >/dev/null 2>&1 || true
}
trap cleanup EXIT

printf '%s\n' '== hatReplication T209 benchmark =='
go test ./hat/hatReplication \
  -run '^$' \
  -bench '^BenchmarkT209RelayBackpressure' \
  -benchtime=300ms \
  -count=5
printf '%s\n' '== hatCache T209 benchmark =='
go test ./hat/hatCache \
  -run '^$' \
  -bench '^BenchmarkT209ReplicationRelayBackpressure' \
  -benchtime=300ms \
  -count=5
