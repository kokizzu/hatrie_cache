#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
format)
	gofmt -w hat/hatQueryHistory/*.go
	;;
test)
	go test ./hat/hatQueryHistory
	;;
race)
	go test -race ./hat/hatQueryHistory
	;;
vet)
	go vet ./hat/hatQueryHistory
	;;
benchmark)
	go test -run '^$' -bench '^BenchmarkMG37' -benchmem -count=5 ./hat/hatQueryHistory
	;;
*)
	echo "usage: $0 {format|test|race|vet|benchmark}" >&2
	exit 2
	;;
esac
