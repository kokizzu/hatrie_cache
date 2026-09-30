#!/bin/sh
set -eu

tmp=$(mktemp -d /tmp/hatrie-cache-chg01-vet.XXXXXX)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/gocache" "$tmp/gotmp"
cd /tmp/hatrie-cache-chg01
GOCACHE="$tmp/gocache" GOTMPDIR="$tmp/gotmp" go vet ./hat/hatSql
