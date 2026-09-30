#!/bin/sh
set -eu

tmp=$(mktemp -d /tmp/hatrie-cache-m231-race.XXXXXX)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/gocache" "$tmp/gotmp"
GOCACHE="$tmp/gocache" GOTMPDIR="$tmp/gotmp" go test -race -run '^TestM231' -count=1 ./hat/hatCache
