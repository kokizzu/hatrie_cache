#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d /tmp/hatrie-m052ah-benchmark.XXXXXX)
trap 'rm -rf "$tmp_dir"' EXIT

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM052AH(AutoNativeCountDistinct|FallbackCountDistinct)$' -benchmem -benchtime=100ms -count=5 >"$tmp_dir/raw.txt"
cat "$tmp_dir/raw.txt"
