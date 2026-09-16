#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-ch007-scheduler-test-XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT
git archive HEAD | tar -x -C "$tmp_dir"
mkdir -p "$tmp_dir/hat/hatSql"
cp hat/hatSql/ch007_ttl_scheduler_test.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/typed_table_ttl_scheduler.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/typed_table_ttl_snapshot.go "$tmp_dir/hat/hatSql/"
cd "$tmp_dir"
go test ./hat/hatSql -run 'TestCH007(TTL|ProcessingTTL)' -count=1 -timeout 10s -v
