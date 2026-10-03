#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-t026-gocache.XXXXXX")"
temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-t026-gotmp.XXXXXX")"
trap 'rm -rf "$cache_dir" "$temp_dir"' EXIT

mode="${1:-test}"
run_go() {
	GOCACHE="$cache_dir" GOTMPDIR="$temp_dir" go "$@"
}

cd "$repo_dir"
case "$mode" in
test)
	run_go test ./hat/hatSchema -run '^TestT026' -count=1
;;
race)
	run_go test -race ./hat/hatSchema -run '^TestT026' -count=1
;;
vet)
	run_go vet ./hat/hatSchema ./hat/hatSql
;;
sql)
	run_go test ./hat/hatSql -run 'IndexHint|Optimizer' -count=1
;;
full-schema)
	run_go test ./hat/hatSchema
;;
*)
	echo "unknown T-U26 verification mode: $mode" >&2
	exit 2
;;
esac
