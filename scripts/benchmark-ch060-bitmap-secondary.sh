#!/usr/bin/env bash
set -euo pipefail

cache=/tmp/hatrie-cache-round63-ch060-benchmark-gocache
trap 'rm -rf -- "$cache"' EXIT
GOCACHE="$cache" go test ./hat/hatCache -run '^$' -bench '^BenchmarkCH060BitmapSecondaryCombination$' -benchmem -count=5
