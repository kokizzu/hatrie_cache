#!/usr/bin/env bash
set -euo pipefail

cache_dir=".t-u09-benchmark-go-cache-$$"
trap 'rm -rf "$cache_dir"' EXIT
export GOTOOLCHAIN=auto
export GOCACHE="$PWD/$cache_dir"

go test ./hat/hatReplication -run '^$' -bench '^BenchmarkJoinBootstrap' -benchmem -benchtime="${BENCHTIME:-250ms}" -count="${COUNT:-5}"
