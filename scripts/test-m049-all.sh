#!/usr/bin/env bash
set -euo pipefail

build_dir=$(mktemp -d /tmp/hatrie-cache-m049-go-build.XXXXXX)
trap 'rm -rf "$build_dir"' EXIT
mkdir -p "$build_dir/tmp"
GOCACHE="$build_dir/cache" GOTMPDIR="$build_dir/tmp" go test ./...
