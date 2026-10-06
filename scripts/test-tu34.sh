#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'running TU34 tests'
gocache=$(mktemp -d /tmp/hatrie-tu34-gocache.XXXXXX)
trap 'rm -rf "$gocache"' EXIT
GOCACHE="$gocache" go test -tags=tu34 ./hat/hatCache -run '^TestTU34' -count=1
