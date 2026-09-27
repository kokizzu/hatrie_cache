#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd -- "$(dirname -- "$0")/.." && pwd)
cache_dir=${GOCACHE:-/tmp/hatrie-cache-gocache-ch037}
tmp_dir=${GOTMPDIR:-/tmp/hatrie-cache-gotmp-ch037-vet}
mkdir -p "$cache_dir" "$tmp_dir"
trap 'rm -rf "$tmp_dir"' EXIT
cd "$root_dir"
GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" go vet ./hat/hatSql
