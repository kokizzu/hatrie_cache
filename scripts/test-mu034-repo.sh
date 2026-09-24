#!/usr/bin/env bash
set -euo pipefail

cache="${TMPDIR:-/tmp}/hatrie-cache-mu034-repo-gocache"
rm -rf "$cache"
mkdir -p "$cache"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test ./...
