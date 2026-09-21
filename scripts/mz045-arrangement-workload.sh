#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
format)
	gofmt -w \
		hat/hatSql/compiled.go \
		hat/hatSql/mz024_arrangement_selection.go \
		hat/hatSql/mz045_arrangement_reuse_test.go \
		hat/hatSql/query.go
	;;
test)
	go test ./hat/hatSql -run '^TestMZ045' -count=1
	;;
benchmark)
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ045ExplainWorkload(CompiledReuse)?$' -benchmem -count=5
	;;
race)
	go test -race ./hat/hatSql -run '^TestMZ045' -count=1
	;;
package)
	go test ./hat/hatSql -count=1
	;;
race-package)
	go test -race ./hat/hatSql -count=1
	;;
vet)
	go vet ./hat/hatSql
	;;
verify)
	go test ./hat/hatSql -run '^TestMZ045' -count=1
	go test -race ./hat/hatSql -run '^TestMZ045' -count=1
	go vet ./hat/hatSql
	;;
*)
	printf 'usage: %s {format|test|benchmark|race|package|race-package|vet|verify}\n' "$0" >&2
	exit 2
	;;
esac
