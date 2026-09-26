#!/usr/bin/env bash
set -euo pipefail

tmp_dir="${TMPDIR:-/tmp}/hatrie-m033c-race"
mkdir -p "$tmp_dir"
trap 'rm -rf "$tmp_dir"' EXIT
GOTMPDIR="$tmp_dir" go test -race ./hat/hatCache ./hat/hatReplication
