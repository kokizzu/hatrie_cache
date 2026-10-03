#!/usr/bin/env bash
set -euo pipefail

tmpdir=$(mktemp -d /tmp/hatrie-mu045-vet.XXXXXX)
cleanup() {
	rm -rf "$tmpdir"
}
trap cleanup EXIT
mkdir -p "$tmpdir/gocache" "$tmpdir/gotmp"
GOCACHE="$tmpdir/gocache" GOTMPDIR="$tmpdir/gotmp" go vet ./hat/hatSql
