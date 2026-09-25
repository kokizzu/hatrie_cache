#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-cache-vet-ch041-grouping-id.XXXXXX)"
trap 'rm -rf -- "$cache_dir"' EXIT
GOCACHE="$cache_dir" go vet ./hat/hatSql
