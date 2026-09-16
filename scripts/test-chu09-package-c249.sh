#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d /tmp/hatrie-cache-chu09-package.XXXXXX)
trap 'rm -rf "$tmp_dir"' EXIT

git archive HEAD | tar -x -C "$tmp_dir"
mkdir -p "$tmp_dir/hat/hatSql" "$tmp_dir/hat/hatCache"
cp hat/hatSql/result_cache_persistence.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/result_cache_persistence_test.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/result_cache_persistence_benchmark_test.go "$tmp_dir/hat/hatSql/"
cp hat/hatCache/sql_result_cache_auto.go "$tmp_dir/hat/hatCache/"
cp hat/hatCache/sql_result_cache_persistence_test.go "$tmp_dir/hat/hatCache/"

(cd "$tmp_dir" && go test ./hat/hatSql ./hat/hatCache -count=1)
