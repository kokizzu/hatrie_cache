#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-cache-mz001-race-XXXXXX)
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test -race ./hat/hatPipeline -count=1
