#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d /tmp/hatrie-mz040-benchmark.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT

mkdir -p "$tmp_dir/gotmp"
printf '%s\n' 'starting MZ040 benchmark'
GOTMPDIR="$tmp_dir/gotmp" go test -json ./hat/hatSql -run '^$' -bench 'BenchmarkMZ040Percentile(RebuildBaseline|Incremental|SingleUpdate)$' -benchmem -count=5
printf '%s\n' 'finished MZ040 benchmark'
