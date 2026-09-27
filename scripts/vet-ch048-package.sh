#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d /tmp/hatrie-ch048-vet.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT
mkdir -p "$tmp_dir/gocache" "$tmp_dir/gotmp"
GOCACHE="$tmp_dir/gocache" GOTMPDIR="$tmp_dir/gotmp" go vet ./hat/hatSql
