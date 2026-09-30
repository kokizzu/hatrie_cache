#!/bin/sh
set -eu

tmp=$(mktemp -d /tmp/hatrie-cache-chg01-race.XXXXXX)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/gocache" "$tmp/gotmp"
cd /tmp/hatrie-cache-chg01
GOCACHE="$tmp/gocache" GOTMPDIR="$tmp/gotmp" go test -race -run '^TestCHG01' -count=1 ./hat/hatSql
