#!/usr/bin/env bash
set -euo pipefail

source_root=.
temporary_root=""
cleanup() {
	if [[ -n "$temporary_root" ]]; then
		rm -rf "$temporary_root"
	fi
}
trap cleanup EXIT

if [[ ! -f hat/hatSql/asof_join.go ]]; then
	temporary_root="$(mktemp -d)"
	cp -a go.mod go.sum hat "$temporary_root/"
	git show HEAD:hat/hatSql/asof_join.go > "$temporary_root/hat/hatSql/asof_join.go"
	source_root="$temporary_root"
fi

(
	cd "$source_root"
	go vet ./hat/hatSql
)
