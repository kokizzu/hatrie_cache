#!/usr/bin/env bash
set -euo pipefail

cache=$(mktemp -d /tmp/hatrie-cache-m045-race.XXXXXX)
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test -race ./hat/hatSql -run '^TestM045' -count=1
