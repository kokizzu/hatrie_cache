#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d /tmp/hatrie-cache-chu17-race.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT

git archive HEAD | tar -x -C "$tmp_dir"
git show HEAD:hat/hatSql/asof_join.go > "$tmp_dir/hat/hatSql/asof_join.go"
cp hat/hatSql/in_program.go "$tmp_dir/hat/hatSql/in_program.go"
cp hat/hatSql/chu17_in_program_test.go "$tmp_dir/hat/hatSql/chu17_in_program_test.go"
(cd "$tmp_dir" && go test -race ./hat/hatSql -run 'TestCHU17' -count=1)
