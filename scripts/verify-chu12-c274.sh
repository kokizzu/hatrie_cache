#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu12-verify-XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT
mkdir -p "$tmp_dir/repo/hat/hatSql"
git archive HEAD | tar -x -C "$tmp_dir/repo"
cp hat/hatSql/index_rebuild_queue.go "$tmp_dir/repo/hat/hatSql/"
cp hat/hatSql/ch_u12_index_rebuild_queue_test.go "$tmp_dir/repo/hat/hatSql/"
cd "$tmp_dir/repo"
go test ./hat/hatSql -run 'TestSQLIndexRebuildQueue' -count=1 -timeout 10s
go test -race ./hat/hatSql -run 'TestSQLIndexRebuildQueue' -count=1 -timeout 30s
go vet ./hat/hatSql
