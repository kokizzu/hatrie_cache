#!/usr/bin/env bash
set -euo pipefail

gofmt -d \
  hat/hatSql/recursive_reachability.go \
  hat/hatSql/recursive_reachability_test.go \
  hat/hatSql/recursive_reachability_benchmark_test.go
go test ./hat/hatSql -run '^TestIncrementalRecursiveReachability' -count=1
go test -race ./hat/hatSql -run '^TestIncrementalRecursiveReachability' -count=1
go vet ./hat/hatSql
git diff --check
git status --short
