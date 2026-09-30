#!/bin/sh
set -eu

tmp=$(mktemp -d /tmp/hatrie-cache-m235-race.XXXXXX)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/gocache" "$tmp/gotmp"
cd /tmp/hatrie-cache-m235
GOCACHE="$tmp/gocache" GOTMPDIR="$tmp/gotmp" go test -race -run '^TestM235' -count=1 ./hat/hatSql
