#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
format)
	shift
	gofmt -w hat/hatSql/mz016_schema_evolution.go hat/hatSql/mz016_schema_evolution_test.go hat/hatSql/mz016_schema_evolution_benchmark_test.go
    ;;
test)
	go test ./hat/hatSql -run '^TestMZ016' -count=1
    ;;
package)
	go test ./hat/hatSql -count=1
    ;;
benchmark)
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ016SchemaEvolution' -benchmem -count=5
    ;;
race)
	go test -race ./hat/hatSql -run '^TestMZ016' -count=1
    ;;
vet)
	go vet ./hat/hatSql
    ;;
verify)
	bash "$0" format
	bash "$0" test
	bash "$0" race
	bash "$0" vet
    ;;
*)
	printf 'usage: %s {format|test|package|benchmark|race|vet|verify}\n' "$0" >&2
	exit 2
    ;;
esac
