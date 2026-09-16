#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu18-red-XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

git archive HEAD | tar -x -C "$tmp_dir"
cp hat/hatSql/ch_u18_composite_primary_test.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/contracts.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/query.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/typed_table.go "$tmp_dir/hat/hatSql/"
(
	cd "$tmp_dir"
	go test ./hat/hatSql -run '^TestSQLColumnarCompositeSparsePrimary|^TestTypedTable.*CompositeSparsePrimary' -count=1
)
