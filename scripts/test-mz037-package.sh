#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d /tmp/hatrie-mz037-package.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT
mkdir -p "$tmp_dir/cache" "$tmp_dir/tmp"
GOCACHE="$tmp_dir/cache" GOTMPDIR="$tmp_dir/tmp" go test ./hat/hatSql -count=1
