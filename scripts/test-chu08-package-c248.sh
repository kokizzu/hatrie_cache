#!/usr/bin/env bash
set -euo pipefail

repo_dir=$PWD
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-chu08-package.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

git archive --format=tar HEAD | tar -xf - -C "$tmp_dir"
cp "$repo_dir/hat/hatCache/ch008_auto_result_cache_test.go" "$tmp_dir/hat/hatCache/"
cp "$repo_dir/hat/hatCache/main.go" "$tmp_dir/hat/hatCache/"
cp "$repo_dir/hat/hatCache/sql_query.go" "$tmp_dir/hat/hatCache/"
cp "$repo_dir/hat/hatCache/sql_result_cache_auto.go" "$tmp_dir/hat/hatCache/"
cd "$tmp_dir"
go test ./hat/hatCache -count=1
