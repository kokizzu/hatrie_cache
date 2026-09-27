#!/usr/bin/env bash
set -euo pipefail
cache_dir="/tmp/hatrie-cache-test-m090d"
tmp_dir="/tmp/hatrie-tmp-test-m090d"
cleanup() {
	rm -rf "$cache_dir" "$tmp_dir"
}
trap cleanup EXIT
mkdir -p "$cache_dir" "$tmp_dir"
GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" go test ./hat/hatSql -run 'TestM090d' -count=1
