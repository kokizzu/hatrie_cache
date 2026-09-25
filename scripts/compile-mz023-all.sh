#!/usr/bin/env bash
set -euo pipefail

cache=/tmp/hatrie-cache-mz023-compile-gocache
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test -run '^$' ./...
