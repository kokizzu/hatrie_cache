#!/usr/bin/env bash
set -euo pipefail

tmp_dir="${TMPDIR:-/tmp}/hatrie-m033c-bench"
mkdir -p "$tmp_dir"
trap 'rm -rf "$tmp_dir"' EXIT
GOTMPDIR="$tmp_dir" go test ./hat/hatCache -run '^$' -bench 'BenchmarkM033cGlobalTimestampReserve' -benchmem -benchtime=1s -count=3
