#!/usr/bin/env bash
set -euo pipefail
cache=$(mktemp -d /tmp/hatrie-cache-tt030-consumers-XXXXXX)
trap 'rm -rf -- "$cache"' EXIT
GOCACHE="$cache" go test ./hat/hatSql -count=1
