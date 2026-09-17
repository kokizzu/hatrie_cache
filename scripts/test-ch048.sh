#!/bin/sh
set -eu

case "${1:-test}" in
test)
	go test ./hat/hatSql -run 'TestCH048'
	;;
package)
	go test ./hat/hatSql
	;;
benchmark)
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH048StringComparison$' -benchmem -count=5
	;;
race)
	go test -race ./hat/hatSql -run 'TestCH048'
	;;
vet)
	go vet ./hat/hatSql
	;;
format)
	gofmt -w hat/hatSql/columnar_string_predicate.go hat/hatSql/ch048_string_predicate_test.go
	;;
*)
	printf '%s\n' "usage: $0 {test|package|benchmark|race|vet|format}" >&2
	exit 2
	;;
esac
