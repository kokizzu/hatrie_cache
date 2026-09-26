#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c153g-bench.XXXXXX")"
trap 'rm -rf -- "$tmp_dir"' EXIT
mkdir -p "$tmp_dir/gocache" "$tmp_dir/gotmp"

GOCACHE="$tmp_dir/gocache" \
GOTMPDIR="$tmp_dir/gotmp" \
go test ./hat/hatCache -run '^$' -bench '^BenchmarkC153gPartitionOwnershipConsensus' -benchmem -count=3 -benchtime=200ms
