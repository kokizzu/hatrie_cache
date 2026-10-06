#!/usr/bin/env bash
set -euo pipefail
cache_dir=/tmp/hatrie-tu19-vet-cache-20261006
rm -rf "$cache_dir"
mkdir -p "$cache_dir"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go vet ./hat/hatCache ./hat/hatDataStructure
