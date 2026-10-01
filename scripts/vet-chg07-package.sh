#!/usr/bin/env bash
set -euo pipefail

cache="$(mktemp -d /tmp/hatrie-cache-chg07-vet-gocache.XXXXXX)"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go vet ./hat/hatSql
