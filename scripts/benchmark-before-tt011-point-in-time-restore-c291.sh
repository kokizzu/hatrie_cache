#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
git archive HEAD | tar -x -C "$tmp_dir"
cp "$(pwd)/hat/hatCache/tt011_restore_benchmark_test.go" "$tmp_dir/hat/hatCache/tt011_restore_benchmark_test.go"
cd "$tmp_dir"
go test ./hat/hatCache -run '^$' -bench '^BenchmarkTT011RestoreDefault$' -benchtime=10x -benchmem -count=5
