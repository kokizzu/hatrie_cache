#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-cache-test-ch041-grouping-id-package.XXXXXX)"
trap 'rm -rf -- "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -count=1
