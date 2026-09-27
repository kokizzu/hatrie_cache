#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-go-cache.XXXXXX")"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatCache -run '^$' -bench '^BenchmarkCH016AsyncInsert/' -benchtime=100ms -count=5
