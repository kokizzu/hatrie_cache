#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  bash scripts/cleanup-go-build-tmp-safe.sh plan >/dev/null 2>&1 || true
  bash scripts/cleanup-go-build-tmp-safe.sh apply >/dev/null 2>&1 || true
  bash scripts/cleanup-go-build-tmp-safe.sh clean-metadata >/dev/null 2>&1 || true
}
trap cleanup EXIT

go test -race ./hat/hatReplication -run '^TestT209' -count=1
go test -race ./hat/hatCache -run '^TestT209' -count=1
