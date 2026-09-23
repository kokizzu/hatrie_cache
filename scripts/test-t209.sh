#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  bash scripts/cleanup-go-build-tmp-safe.sh plan >/dev/null 2>&1 || true
  bash scripts/cleanup-go-build-tmp-safe.sh apply >/dev/null 2>&1 || true
  bash scripts/cleanup-go-build-tmp-safe.sh clean-metadata >/dev/null 2>&1 || true
}
trap cleanup EXIT

printf '%s\n' '== hatReplication T209 tests =='
go test ./hat/hatReplication -run '^TestT209' -count=1
printf '%s\n' '== hatCache T209 tests =='
go test ./hat/hatCache -run '^TestT209' -count=1
