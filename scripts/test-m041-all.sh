#!/usr/bin/env bash
set -euo pipefail

cache_dir="/tmp/hatrie-cache-m041-all-cache-$$"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./...
