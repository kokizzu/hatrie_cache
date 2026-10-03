#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TMP=$(mktemp -d /tmp/hatrie-mu040-benchmark.XXXXXX)
GOCACHE="$TMP/gocache"
GOTMPDIR="$TMP/gotmp"
mkdir -p "$GOCACHE" "$GOTMPDIR"
cleanup() {
	rm -rf "$TMP"
}
trap cleanup EXIT
cd "$ROOT"
GOTMPDIR="$GOTMPDIR" GOCACHE="$GOCACHE" go test ./hat/hatSchema -run '^$' -bench '^BenchmarkMU40' -benchmem -count=3
