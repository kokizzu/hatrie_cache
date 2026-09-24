#!/usr/bin/env bash
set -euo pipefail

cache_dir="/tmp/hatrie-cache-m039-go-cache-$$"
trap 'rm -rf "$cache_dir"' EXIT
mkdir -p "$cache_dir"
GOCACHE="$cache_dir" go test ./hat/hatSql -count=1
