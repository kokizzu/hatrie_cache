#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu11-before-XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

git archive HEAD | tar -x -C "$tmp_dir"
(cd "$tmp_dir" && go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQLIndexAdvisorCovering$' -benchmem -count=5)
