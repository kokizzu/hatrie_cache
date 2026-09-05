#!/bin/sh
set -eu

mode=${1:-test}

case "$mode" in
test)
  go test ./hat/hatSql -run '^TestSQLRowBinaryDateAndDateTimeUseFixedWidthEncoding$' -count=1
  ;;
race)
  go test -race ./hat/hatSql -run '^TestSQLRowBinaryDateAndDateTimeUseFixedWidthEncoding$' -count=1
  ;;
format)
  gofmt -w hat/hatSql/row_binary_fixed_width_test.go
  ;;
*)
  printf '%s\n' "usage: $0 [test|race|format]" >&2
  exit 2
  ;;
esac
