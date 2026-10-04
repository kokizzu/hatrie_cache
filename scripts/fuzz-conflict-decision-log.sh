#!/bin/sh
set -eu

cache=/tmp/hatrie-tu38-fuzz-cache
tmpdir=/tmp/hatrie-tu38-fuzz-tmp
cleanup() {
	rm -rf "$cache" "$tmpdir"
}
trap cleanup EXIT INT TERM
rm -rf "$cache" "$tmpdir"
mkdir -p "$cache" "$tmpdir"
GOCACHE="$cache" GOTMPDIR="$tmpdir" go test ./hat/hatReplication -run '^$' -fuzz '^FuzzRestoreConflictDecisionLogNeverPanics$' -fuzztime=2s
