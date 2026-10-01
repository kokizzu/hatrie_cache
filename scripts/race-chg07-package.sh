#!/usr/bin/env bash
set -euo pipefail

cache="$(mktemp -d /tmp/hatrie-cache-chg07-race-gocache.XXXXXX)"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test -race ./hat/hatSql
