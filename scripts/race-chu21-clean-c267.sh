#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu21-race-XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT
git archive HEAD | tar -x -C "$tmp_dir"
cp hat/hatSql/external.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/ch_u21_streaming_import_test.go "$tmp_dir/hat/hatSql/"
(
	cd "$tmp_dir"
	go test -race ./hat/hatSql -count=1
)
