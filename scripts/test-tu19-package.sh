#!/usr/bin/env bash
set -euo pipefail
cache_dir=/tmp/hatrie-tu19-test-package-cache-20261006
rm -rf "$cache_dir"
mkdir -p "$cache_dir"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test -vet=off -p=4 ./hat/hatCache ./hat/hatDataStructure
