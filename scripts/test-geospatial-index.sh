#!/bin/sh
set -eu

mode=${1:-test}

case "$mode" in
all)
  go test ./hat/hatSql -count=1
  ;;
test)
  go test ./hat/hatSql -run '^TestGeoIndexWithinBoxDoesNotDuplicateDatelineCandidates$' -count=1
  ;;
race)
  go test -race ./hat/hatSql -run '^TestGeoIndexWithinBoxDoesNotDuplicateDatelineCandidates$' -count=1
  ;;
bench)
  go test ./hat/hatSql -run '^$' -bench '^BenchmarkGeoIndexWithinBox(CandidateCollection|SparseWide)$' -benchmem -benchtime=1s -count=3
  ;;
format)
  gofmt -w hat/hatSql/geospatial_optimization_test.go
  ;;
*)
  printf '%s\n' "usage: $0 [all|test|race|bench|format]" >&2
  exit 2
  ;;
esac
