#!/usr/bin/env bash
set -euo pipefail
cache_dir=$(mktemp -d /tmp/hatrie-tt006-gocache.XXXXXX)
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./... -run '^$'
