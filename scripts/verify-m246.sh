#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-cache-m246-verify.XXXXXX)"
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test ./...
