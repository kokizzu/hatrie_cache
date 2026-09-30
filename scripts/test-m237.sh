#!/bin/sh
set -eu

tmp=$(mktemp -d /tmp/hatrie-cache-m237-test.XXXXXX)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/gocache" "$tmp/gotmp"
cd /tmp/hatrie-cache-m237
GOCACHE="$tmp/gocache" GOTMPDIR="$tmp/gotmp" go test -run '^TestM237' -count=1 ./hat/hatSql
