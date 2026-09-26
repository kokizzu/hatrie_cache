#!/usr/bin/env bash
set -euo pipefail

tmp_dir="${TMPDIR:-/tmp}/hatrie-m033c-focused-race"
mkdir -p "$tmp_dir"
trap 'rm -rf "$tmp_dir"' EXIT
GOTMPDIR="$tmp_dir" go test -race ./hat/hatCache -run 'TestM033cGlobalTimestampReserveGRPC' -count=1
