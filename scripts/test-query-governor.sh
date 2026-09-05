#!/bin/sh
set -eu

mode=${1:-test}

case "$mode" in
bench)
  go test ./hat/hatSql -run '^$' -bench '^BenchmarkNamespaceQueryGateFastPath$' -benchmem -benchtime=1s -count=3
  ;;
test)
  go test ./hat/hatSql -run '^TestNamespaceQueryGovernor' -count=1
  ;;
race)
  go test -race ./hat/hatSql -run '^TestNamespaceQueryGovernor' -count=1
  ;;
format)
  gofmt -w hat/hatSql/governance_queue_test.go
  ;;
*)
  printf '%s\n' "usage: $0 [bench|test|race|format]" >&2
  exit 2
  ;;
esac
