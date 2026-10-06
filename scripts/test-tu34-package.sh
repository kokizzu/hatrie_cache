#!/usr/bin/env bash
set -euo pipefail

gocache=$(mktemp -d /tmp/hatrie-tu34-gocache.XXXXXX)
trap 'rm -rf "$gocache"' EXIT
GOCACHE="$gocache" go test -tags=tu34 ./hat/hatCache -count=1
