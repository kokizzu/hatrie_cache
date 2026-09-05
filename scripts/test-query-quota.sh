#!/bin/sh
set -eu

mode=${1:-test}

case "$mode" in
test)
  go test ./hat/hatSql -run '^TestNamespaceQueryQuota' -count=1
  ;;
race)
  go test -race ./hat/hatSql -run '^TestNamespaceQueryQuota' -count=1
  ;;
bench)
  go test ./hat/hatSql -run '^$' -bench '^BenchmarkNamespaceQueryQuotaDisabledPath$' -benchmem -benchtime=1s -count=3
  ;;
format)
  gofmt -w hat/hatSql/governance_quota_test.go
  ;;
*)
  printf '%s\n' "usage: $0 [test|race|bench|format]" >&2
  exit 2
  ;;
esac
