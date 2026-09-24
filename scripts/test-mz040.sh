#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d /tmp/hatrie-mz040-test.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT

mkdir -p "$tmp_dir/gotmp"
GOTMPDIR="$tmp_dir/gotmp" go test ./hat/hatSql -run 'TestMZ040|TestIncrementalPercentile' -count=1
