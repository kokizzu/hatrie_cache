#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu09-hatcache-test.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

git archive --format=tar HEAD | tar -xf - -C "$tmp_dir"
cp hat/hatSql/result_cache_persistence.go "$tmp_dir/hat/hatSql/"
cp hat/hatCache/sql_result_cache_auto.go "$tmp_dir/hat/hatCache/"
cp hat/hatCache/sql_result_cache_persistence_test.go "$tmp_dir/hat/hatCache/"
cd "$tmp_dir"
go test ./hat/hatCache -run 'TestHatTrieSQLResultCachePersistence' -count=1
