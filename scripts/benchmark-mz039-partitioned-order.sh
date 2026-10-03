#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d /tmp/hatrie-mz039-benchmark.XXXXXX)"
cleanup() {
	rm -rf "$TMP"
}
trap cleanup EXIT

mkdir -p "$TMP/gotmp" "$TMP/gocache"
cd "$ROOT"
env GOTMPDIR="$TMP/gotmp" GOCACHE="$TMP/gocache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ39PartitionedOrder' -benchmem -count=5
