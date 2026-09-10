#!/usr/bin/env bash
set -euo pipefail

if [[ -n "$(gofmt -l \
  hat/hatSql/incremental_rank_window.go \
  hat/hatSql/m065_rank_window_benchmark_test.go \
  hat/hatSql/m065_rank_window_test.go)" ]]; then
  printf '%s\n' 'M065 rank-window Go files are not gofmt-formatted' >&2
  exit 1
fi

go vet ./hat/hatSql
go test -race ./hat/hatSql -run '^TestIncrementalRankWindow' -count=1
git diff --check
git status --short
