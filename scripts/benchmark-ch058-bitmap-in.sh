#!/usr/bin/env bash
set -euo pipefail

cache_dir=/tmp/hatrie-cache-round61-ch058-gocache
rm -rf -- "$cache_dir"
trap 'rm -rf -- "$cache_dir"' EXIT
mkdir -p -- "$cache_dir"
GOCACHE="$cache_dir" go test ./hat/hatCache -run '^$' -bench '^BenchmarkCH058BitmapIndexedIN' -benchmem -benchtime=200ms -count=3
