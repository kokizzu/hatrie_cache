#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-mz004-gocache.XXXXXX)
trap 'rm -rf -- "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./...
