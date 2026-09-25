#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
cache_dir="/tmp/hatrie-cache-ch002-vet-gocache"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go vet ./hat/hatSql
