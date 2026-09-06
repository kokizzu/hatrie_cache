#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master

printf '%s\n' '== hatMetrics codec accounting =='
go test ./hat/hatMetrics
printf '%s\n' '== hatMetrics codec accounting race =='
go test -race ./hat/hatMetrics
printf '%s\n' '== SQL RowBinary accounting focused tests =='
go test ./hat/hatSql/row_binary_codec_accounting.go ./hat/hatSql/row_binary_codec_accounting_test.go
printf '%s\n' '== SQL RowBinary accounting focused race =='
go test -race ./hat/hatSql/row_binary_codec_accounting.go ./hat/hatSql/row_binary_codec_accounting_test.go
