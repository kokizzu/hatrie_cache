#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu15-vet-XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

git archive HEAD | tar -x -C "$tmp_dir"
cp hat/hatSql/decimal_kernels.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/decimal_types.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/ch_u15_decimal_kernels_test.go "$tmp_dir/hat/hatSql/"
(
	cd "$tmp_dir"
	go vet ./hat/hatSql
)
