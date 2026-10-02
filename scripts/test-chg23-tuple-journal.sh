#!/usr/bin/env bash
set -euo pipefail

mode=${1:-test}
case "$mode" in
test)
	go test ./hat/hatDataStructure -run 'TestTupleFieldUpdateJournal' -count=1
	;;
all)
	go test ./hat/hatDataStructure -count=1
	;;
race)
	go test -race ./hat/hatDataStructure -run 'TestTupleFieldUpdateJournal' -count=1
	;;
vet)
	go vet ./hat/hatDataStructure
	;;
package)
	go test ./hat/hatDataStructure -run '^$' -count=1
	;;
benchmark)
	go test ./hat/hatDataStructure -run '^$' -bench 'BenchmarkTupleFieldUpdateJournal' -benchmem -count=1
	;;
format)
	go fmt ./hat/hatDataStructure
	;;
*)
	printf 'unsupported mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
