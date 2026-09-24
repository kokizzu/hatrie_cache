#!/usr/bin/env bash
set -euo pipefail

cache_dir="/tmp/hatrie-cache-m041-package-cache-$$"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -count=1
