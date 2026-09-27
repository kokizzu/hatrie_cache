#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d /tmp/hatrie-ch048-all.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT
mkdir -p "$tmp_dir/gocache" "$tmp_dir/gotmp"
GOCACHE="$tmp_dir/gocache" GOTMPDIR="$tmp_dir/gotmp" go test ./... -count=1
