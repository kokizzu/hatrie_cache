#!/usr/bin/env bash
set -euo pipefail
cache_dir=/tmp/hatrie-tu19-race-cache-20261006
rm -rf "$cache_dir"
mkdir -p "$cache_dir"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test -race -vet=off -p=1 ./hat/hatCache -run '^TestTU19' -count=1
