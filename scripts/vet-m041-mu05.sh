#!/usr/bin/env bash
set -euo pipefail

cache_dir="/tmp/hatrie-cache-m041-vet-cache-$$"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go vet ./hat/hatSql
