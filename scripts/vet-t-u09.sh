#!/usr/bin/env bash
set -euo pipefail

cache_dir=".t-u09-vet-go-cache-$$"
trap 'rm -rf "$cache_dir"' EXIT
export GOTOOLCHAIN=auto
export GOCACHE="$PWD/$cache_dir"

go vet ./hat/hatReplication
