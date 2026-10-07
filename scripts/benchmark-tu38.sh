#!/usr/bin/env bash
set -euo pipefail

action="${1:-benchmark}"
case "$action" in
baseline)
	go test -run '^$' -bench '^BenchmarkConflictPolicyResolution$' -benchmem -benchtime=100ms -count=5 ./hat/hatReplication
	;;
benchmark)
	go test -run '^$' -bench '^Benchmark(ConflictPolicyResolution|TU38ConflictInspection)' -benchmem -benchtime=100ms -count=5 ./hat/hatReplication
	;;
test)
	go test -run '^TestTU38' -count=1 ./hat/hatReplication
	;;
race)
	go test -race -run '^TestTU38' -count=1 ./hat/hatReplication
	;;
vet)
	go vet ./hat/hatReplication
	;;
*)
	echo "unknown action: $action" >&2
	exit 2
	;;
esac
