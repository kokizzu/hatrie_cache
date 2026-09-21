#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

mode="${1:-verify}"
case "$mode" in
format)
	gofmt -w \
		hat/hatReplication/tt006_hot_standby.go \
		hat/hatReplication/tt006_hot_standby_benchmark_test.go \
		hat/hatReplication/tt006_hot_standby_test.go
	;;
test)
	go test ./hat/hatReplication -run '^TestTT006HotStandby'
	;;
package)
	go test ./hat/hatReplication
	;;
race)
	go test -race ./hat/hatReplication -run '^TestTT006HotStandby'
	;;
race-package)
	go test -race ./hat/hatReplication
	;;
benchmark)
	go test ./hat/hatReplication -run '^$' -bench '^BenchmarkTT006' -benchmem -count=5
	;;
vet)
	go vet ./hat/hatReplication
	;;
verify)
	bash "$0" format
	bash "$0" test
	bash "$0" package
	bash "$0" race
	bash "$0" vet
	;;
*)
	printf 'unknown tt006 mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
