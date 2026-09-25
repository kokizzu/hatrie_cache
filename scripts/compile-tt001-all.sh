#!/usr/bin/env bash
set -euo pipefail

cache=/tmp/hatrie-cache-tt001-compile-gocache
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test -run '^$' ./...
