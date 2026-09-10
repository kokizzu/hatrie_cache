#!/usr/bin/env bash
set -euo pipefail

if [[ -n "$(gofmt -l \
  hat/hatSql/sql_snapshot_provider.go \
  hat/hatSql/sql_snapshot_provider_test.go \
  hat/hatSql/sql_snapshot_provider_benchmark_test.go)" ]]; then
  printf '%s\n' 'snapshot provider Go files are not gofmt-formatted' >&2
  exit 1
fi

go vet ./hat/hatSql
go test -race ./hat/hatSql -run '^TestSQLSnapshotProvider' -count=1
git diff --check
git status --short
