#!/usr/bin/env bash
set -euo pipefail

cache_dir=".t-u09-test-go-cache-$$"
trap 'rm -rf "$cache_dir"' EXIT
export GOTOOLCHAIN=auto
export GOCACHE="$PWD/$cache_dir"

go test ./hat/hatReplication -run '^TestJoinBootstrap' -count=1
