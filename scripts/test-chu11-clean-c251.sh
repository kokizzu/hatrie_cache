#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu11-XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

git archive HEAD | tar -x -C "$tmp_dir"
git diff --binary HEAD -- \
  hat/hatSql/index_advisor.go \
  hat/hatSql/index_advisor_persistence.go \
  hat/hatSql/index_advisor_persistence_test.go \
  > "$tmp_dir/ch-u11.patch"
git -C "$tmp_dir" apply "$tmp_dir/ch-u11.patch"
cp hat/hatSql/ch_u11_skip_index_advisor_test.go "$tmp_dir/hat/hatSql/ch_u11_skip_index_advisor_test.go"

(cd "$tmp_dir" && go test ./hat/hatSql -run 'TestCHU11SQLIndexAdvisor' -count=1)
