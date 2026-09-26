#!/usr/bin/env bash
set -euo pipefail

tmp_dir="${TMPDIR:-/tmp}/hatrie-m033c-test"
mkdir -p "$tmp_dir"
trap 'rm -rf "$tmp_dir"' EXIT
GOTMPDIR="$tmp_dir" go test ./hat/hatCache -run 'TestM033cGlobalTimestampReserveGRPC' -count=1
