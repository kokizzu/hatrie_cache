#!/usr/bin/env bash
set -euo pipefail
cache_dir="/tmp/hatrie-cache-vet-m090d"
tmp_dir="/tmp/hatrie-tmp-vet-m090d"
cleanup() {
	rm -rf "$cache_dir" "$tmp_dir"
}
trap cleanup EXIT
mkdir -p "$cache_dir" "$tmp_dir"
GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" go vet ./hat/hatSql
