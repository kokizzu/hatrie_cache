#!/usr/bin/env bash
set -euo pipefail

cache="${TMPDIR:-/tmp}/hatrie-cache-round62-ch059-benchmark-gocache"
rm -rf "$cache"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test ./hat/hatCache -run '^$' -bench '^BenchmarkCH059BitmapIndexedEquality$' -benchmem -benchtime=200ms -count=3
