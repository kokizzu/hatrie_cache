#!/bin/sh
set -eu

case "${1:-}" in
format)
	gofmt -w hat/hatReplication/tt005_raft_configuration_state.go hat/hatReplication/tt005_raft_configuration_state_test.go hat/hatReplication/tt005_raft_configuration_state_benchmark_test.go
	;;
test)
	go test ./hat/hatReplication -run '^TestTT005Configuration' -count=1
	;;
package)
	go test ./hat/hatReplication
	;;
race)
	go test -race ./hat/hatReplication -run '^TestTT005Configuration' -count=1
	;;
race-package)
	go test -race ./hat/hatReplication
	;;
benchmark)
	go test ./hat/hatReplication -run '^$' -bench '^BenchmarkTT005' -benchmem -count=5
	;;
review)
	git diff --cached --check
	git diff --cached --stat
	git diff --cached --name-only
	;;
vet)
	go vet ./hat/hatReplication
	;;
verify)
	bash "$0" format
	bash "$0" test
	bash "$0" race
	bash "$0" vet
	;;
*)
	printf '%s\n' 'usage: tt005-raft-configuration.sh {format|test|package|race|race-package|benchmark|review|vet|verify}' >&2
	exit 2
	;;
esac
