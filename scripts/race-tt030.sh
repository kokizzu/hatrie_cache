#!/usr/bin/env bash
set -euo pipefail
cache=$(mktemp -d /tmp/hatrie-cache-tt030-race-XXXXXX)
trap 'rm -rf -- "$cache"' EXIT
GOCACHE="$cache" go test -race ./hat/hatSchema -count=1
