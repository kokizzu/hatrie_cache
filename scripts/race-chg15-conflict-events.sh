#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chg15-race.XXXXXX")"
cleanup() {
	rm -rf "$tmp_dir"
}
trap cleanup EXIT
mkdir -p "$tmp_dir/go-build" "$tmp_dir/go-tmp" "$tmp_dir/tmp"
export GOCACHE="$tmp_dir/go-build"
export GOTMPDIR="$tmp_dir/go-tmp"
export TMPDIR="$tmp_dir/tmp"
go test -race ./hat/hatReplication -run 'TestConflictEventLog' -count=1
