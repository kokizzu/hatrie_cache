#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-ch036-gocache.XXXXXX)"
tmp_dir="$(mktemp -d /tmp/hatrie-ch036-gotmp.XXXXXX)"
cleanup() {
	rm -rf "$cache_dir" "$tmp_dir"
}
trap cleanup EXIT
export GOCACHE="$cache_dir"
export GOTMPDIR="$tmp_dir"
go test ./hat/hatSql -run '^(TestCH036AsofPreparation|TestSQLAsofJoin)' -count=1
