#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-m039-test-cache.XXXXXX)
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^TestM039SQL' -count=1
