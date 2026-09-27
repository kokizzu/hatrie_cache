#!/usr/bin/env bash
set -euo pipefail
cache_dir="/tmp/hatrie-cache-race-m090d"
tmp_dir="/tmp/hatrie-tmp-race-m090d"
cleanup() {
	rm -rf "$cache_dir" "$tmp_dir"
}
trap cleanup EXIT
mkdir -p "$cache_dir" "$tmp_dir"
GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" go test -race ./hat/hatSql -run 'TestM090d' -count=1
