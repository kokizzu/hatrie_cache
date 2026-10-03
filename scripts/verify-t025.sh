#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cache_dir="$(mktemp -d /tmp/hatrie-cache-verify-t025.XXXXXX)"
temp_dir="$(mktemp -d /tmp/hatrie-cache-verify-t025-go.XXXXXX)"
trap 'rm -rf "$cache_dir" "$temp_dir"' EXIT

cd "$repo_dir"

case "${1:-test}" in
test)
	GOCACHE="$cache_dir" GOTMPDIR="$temp_dir" go test ./hat/hatSchema -run '^TestT025' -count=1
	;;
race)
	GOCACHE="$cache_dir" GOTMPDIR="$temp_dir" go test -race ./hat/hatSchema -run '^TestT025' -count=1
	;;
vet)
	GOCACHE="$cache_dir" GOTMPDIR="$temp_dir" go vet ./hat/hatSchema
	;;
full)
	GOCACHE="$cache_dir" GOTMPDIR="$temp_dir" go test ./hat/hatSchema ./hat/hatSql
	;;
*)
	printf 'unknown T-U25 verification mode: %s\n' "$1" >&2
	exit 2
	;;
esac
