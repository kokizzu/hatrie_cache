#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
format)
	"$(go env GOROOT)/bin/gofmt" -w hat/hatSandbox/*.go
	;;
test)
	go test ./hat/hatSandbox
	;;
race)
	go test -race ./hat/hatSandbox
	;;
vet)
	go vet ./hat/hatSandbox
	;;
benchmark)
	go test -run '^$' -bench '^BenchmarkTG50' -benchmem -count=5 ./hat/hatSandbox
	;;
*)
	echo "usage: $0 {format|test|race|vet|benchmark}" >&2
	exit 2
	;;
esac
