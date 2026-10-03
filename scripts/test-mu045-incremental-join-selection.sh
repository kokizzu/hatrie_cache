#!/usr/bin/env bash
set -euo pipefail

tmpdir=$(mktemp -d /tmp/hatrie-mu045-test.XXXXXX)
cleanup() {
	rm -rf "$tmpdir"
}
trap cleanup EXIT
mkdir -p "$tmpdir/gocache" "$tmpdir/gotmp"
GOCACHE="$tmpdir/gocache" GOTMPDIR="$tmpdir/gotmp" go test ./hat/hatSql -run '^TestMU045' -count=1
