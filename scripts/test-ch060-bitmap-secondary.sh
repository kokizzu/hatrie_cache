#!/usr/bin/env bash
set -euo pipefail

cache=/tmp/hatrie-cache-round63-ch060-test-gocache
trap 'rm -rf -- "$cache"' EXIT
GOCACHE="$cache" go test ./hat/hatCache -run '^TestCH060BitmapSecondaryCombination$' -count=1
