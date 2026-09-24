#!/usr/bin/env bash
set -euo pipefail

cache=$(mktemp -d /tmp/hatrie-cache-m046-vet.XXXXXX)
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go vet ./hat/hatSql
