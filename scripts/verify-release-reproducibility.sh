#!/bin/sh
set -eu

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-release.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT
mkdir -p "$work_dir/first" "$work_dir/second"

for command in hatrie-cache hatrie-cli hatrie-sql-lsp; do
	go build -trimpath -buildvcs=false -ldflags=-buildid= -o "$work_dir/first/$command" "./cmd/$command"
	go build -trimpath -buildvcs=false -ldflags=-buildid= -o "$work_dir/second/$command" "./cmd/$command"
	cmp "$work_dir/first/$command" "$work_dir/second/$command"
	sha256sum "$work_dir/first/$command"
done
