#!/bin/sh
set -eu

case "${1:-test}" in
test)
	go test ./hat/hatSql -run 'TestMZ045' -count=1
	;;
package)
	go test ./hat/hatSql -count=1
	;;
benchmark)
	go test ./hat/hatSql -run '^$' -bench 'BenchmarkMZ045' -benchmem -count=5
	;;
race)
	go test -race ./hat/hatSql -run 'TestMZ045' -count=1
	;;
vet)
	go vet ./hat/hatSql
	;;
format)
	gofmt -w hat/hatSql/mz045_plan_equivalence_test.go hat/hatSql/c213_compiled_plan_cache.go
	;;
*)
	printf 'usage: %s {test|package|benchmark|race|vet|format}\n' "$0" >&2
	exit 2
	;;
esac
