#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd -- "$(dirname -- "$0")/.." && pwd)
cache_dir=${GOCACHE:-/tmp/hatrie-cache-gocache-m090e}
tmp_dir=${GOTMPDIR:-/tmp/hatrie-cache-gotmp-m090e-package}
mkdir -p "$cache_dir" "$tmp_dir"
trap 'rm -rf "$tmp_dir"' EXIT
cd "$root_dir"
GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" go test ./hat/hatSql -count=1
