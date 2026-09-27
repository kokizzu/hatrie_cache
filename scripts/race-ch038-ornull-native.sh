#!/usr/bin/env bash
set -euo pipefail

cache_dir="/tmp/hatrie-ch038-ornull-race-gocache"
tmp_dir="/tmp/hatrie-ch038-ornull-race-gotmp"
rm -rf "$cache_dir" "$tmp_dir"
mkdir -p "$cache_dir" "$tmp_dir"
trap 'rm -rf "$cache_dir" "$tmp_dir"' EXIT
GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" go test -race ./hat/hatSql -run '^TestCH038OrNull' -count=1
