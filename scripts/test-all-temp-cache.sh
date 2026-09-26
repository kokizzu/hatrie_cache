#!/usr/bin/env bash
set -euo pipefail

cache=$(mktemp -d /tmp/hatrie-go-cache.XXXXXX)
trap 'rm -rf -- "$cache"' EXIT
GOCACHE="$cache" go test ./... -count=1
