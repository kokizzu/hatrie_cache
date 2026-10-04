#!/bin/sh
set -eu

cache=/tmp/hatrie-tu19-go-build
tmpdir=/tmp/hatrie-tu19-go-tmp
cleanup() {
	rm -rf "$cache" "$tmpdir"
}
trap cleanup EXIT INT TERM
rm -rf "$cache" "$tmpdir"
mkdir -p "$cache" "$tmpdir"
GOCACHE="$cache" GOTMPDIR="$tmpdir" go test ./... "$@"
