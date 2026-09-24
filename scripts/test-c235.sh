#!/usr/bin/env bash
set -euo pipefail

cache_dir="${TMPDIR:-/tmp}/hatrie-cache-c235-gocache"
rm -rf "$cache_dir"
mkdir -p "$cache_dir"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -run 'TestSQLTaskProfiler' -count=1
