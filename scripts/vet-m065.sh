#!/usr/bin/env bash
set -euo pipefail

cache_dir=/tmp/hatrie-cache-m065-vet-gocache
mkdir -p "$cache_dir"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go vet ./hat/hatSql
