#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d /tmp/hatrie-chu06-benchmark.XXXXXX)
cleanup() {
	rm -rf "$WORK"
}
trap cleanup EXIT INT TERM
mkdir -p "$WORK/gotmp" "$WORK/gocache"
cd "$ROOT"
GOTMPDIR="$WORK/gotmp" GOCACHE="$WORK/gocache" go test ./hat/hatSql -run '^$' -bench '^Benchmark(CH005PatchState|CHU06PersistentDeleteBitmap)$' -benchmem -benchtime="${BENCHTIME:-20ms}" -count="${COUNT:-5}"
