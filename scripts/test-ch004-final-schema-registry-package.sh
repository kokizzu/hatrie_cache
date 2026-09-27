#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d /tmp/hatrie-cache-ch004-final-schema-registry-package.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT
mkdir -p "$tmp_dir/go-cache" "$tmp_dir/go-tmp"
GOCACHE="$tmp_dir/go-cache" GOTMPDIR="$tmp_dir/go-tmp" go test ./hat/hatSql -count=1
