#!/usr/bin/env bash
set -euo pipefail

repo_dir=$PWD
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu09-test.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

git archive --format=tar HEAD | tar -xf - -C "$tmp_dir"
cp hat/hatSql/result_cache_persistence.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/result_cache_persistence_test.go "$tmp_dir/hat/hatSql/"
cd "$tmp_dir"
go test ./hat/hatSql -run 'TestSQLResultCachePersistence' -count=1
