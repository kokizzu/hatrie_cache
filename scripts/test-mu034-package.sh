#!/usr/bin/env bash
set -euo pipefail

cache="${TMPDIR:-/tmp}/hatrie-cache-mu034-package-gocache"
rm -rf "$cache"
mkdir -p "$cache"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test ./hat/hatSql -count=1
