#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
format)
	gofmt -w hat/hatResource/*.go
	;;
test)
	go test ./hat/hatResource
	;;
race)
	go test -race ./hat/hatResource
	;;
vet)
	go vet ./hat/hatResource
	;;
benchmark)
	go test -run '^$' -bench '^BenchmarkMG45' -benchmem -count=5 ./hat/hatResource
	;;
*)
	echo "usage: $0 {format|test|race|vet|benchmark}" >&2
	exit 2
	;;
esac
