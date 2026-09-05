#!/bin/sh
set -eu

mode=${1:-test}

case "$mode" in
bench)
  go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLQueryManagerHistoryAppendSampling$' -benchmem -benchtime=1s -count=3
  ;;
test)
  go test ./hat/hatSql -run '^TestSQLQueryManagerHistorySamplingIsDeterministicAndBounded$' -count=1
  ;;
race)
  go test -race ./hat/hatSql -run '^TestSQLQueryManagerHistorySamplingIsDeterministicAndBounded$' -count=1
  ;;
format)
  gofmt -w hat/hatSql/query_manager_history_sampling_test.go
  ;;
*)
  printf '%s\n' "usage: $0 [bench|test|race|format]" >&2
  exit 2
  ;;
esac
