#!/usr/bin/env bash
set -euo pipefail

cache_dir="/tmp/hatrie-ch038-ornull-benchmark-gocache"
tmp_dir="/tmp/hatrie-ch038-ornull-benchmark-gotmp"
rm -rf "$cache_dir" "$tmp_dir"
mkdir -p "$cache_dir" "$tmp_dir"
trap 'rm -rf "$cache_dir" "$tmp_dir"' EXIT
GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH038AutomaticNativeOrNull$' -benchtime=20x -count=5
