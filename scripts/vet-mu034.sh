#!/usr/bin/env bash
set -euo pipefail

cache="${TMPDIR:-/tmp}/hatrie-cache-mu034-vet-gocache"
rm -rf "$cache"
mkdir -p "$cache"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go vet ./hat/hatSql
