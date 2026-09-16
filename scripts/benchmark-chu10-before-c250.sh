#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d /tmp/hatrie-cache-chu10-before.XXXXXX)
trap 'rm -rf "$tmp_dir"' EXIT

git archive HEAD | tar -x -C "$tmp_dir"
(cd "$tmp_dir" && go test -bench='BenchmarkSQLProjectionAdvisor$' -benchmem -benchtime=200ms -count=5 ./hat/hatSql)
