#!/usr/bin/env bash
set -euo pipefail

files=(
  hat/hatSql/mutable_rank_window.go
  hat/hatSql/m065_mutable_rank_window_test.go
  hat/hatSql/m065_rank_window_benchmark_test.go
)

if [[ -n "$(gofmt -l "${files[@]}")" ]]; then
  printf '%s\n' 'M065z Go files are not gofmt-formatted.' >&2
  exit 1
fi
git diff --check
printf '%s\n' 'M065z rank fast-path review passed.'
printf '%s\n' 'Staged paths:'
git diff --cached --name-only
