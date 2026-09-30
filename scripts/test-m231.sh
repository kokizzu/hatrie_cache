#!/bin/sh
set -eu

tmp=$(mktemp -d /tmp/hatrie-cache-m231-test.XXXXXX)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/gocache" "$tmp/gotmp"
GOCACHE="$tmp/gocache" GOTMPDIR="$tmp/gotmp" go test -run '^TestM231' -count=1 ./hat/hatCache
