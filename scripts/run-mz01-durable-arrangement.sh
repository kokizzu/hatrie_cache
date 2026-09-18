#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
format)
	go fmt ./hat/hatDataStructure
	;;
test)
	go test ./hat/hatDataStructure -run 'Test(MZ01|SpillableArrangement)' -count=1
	;;
benchmark-baseline)
	go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkMZ01RebuildFromSnapshot$' -benchmem -count=5
	;;
benchmark)
	go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkMZ01(RebuildFromSnapshot|OpenDurableSegment)$' -benchmem -count=5
	;;
race)
	go test -race ./hat/hatDataStructure -run 'Test(MZ01|SpillableArrangement)' -count=1
	;;
vet)
	go vet ./hat/hatDataStructure
	;;
package)
	go test ./...
	;;
    review)
        git diff --check
        git diff --cached --check
	git status --short
	git diff --stat
	git diff -- BENCHMARK.md INSPIRATION_BACKLOG.md MZ001_DURABLE_PERSISTED_ARRANGEMENT.md README.md hat/hatDataStructure/spillable_arrangement.go hat/hatDataStructure/mz001_durable_arrangement_test.go hat/hatDataStructure/mz001_durable_arrangement_benchmark_test.go scripts/run-mz01-durable-arrangement.sh Makefile
	;;
stage)
	git add BENCHMARK.md INSPIRATION_BACKLOG.md MZ001_DURABLE_PERSISTED_ARRANGEMENT.md README.md hat/hatDataStructure/spillable_arrangement.go hat/hatDataStructure/mz001_durable_arrangement_test.go hat/hatDataStructure/mz001_durable_arrangement_benchmark_test.go scripts/run-mz01-durable-arrangement.sh
	git apply --cached --check /tmp/mz01-makefile-stage.patch
	git apply --cached /tmp/mz01-makefile-stage.patch
	rm -f /tmp/mz01-makefile-stage.patch
	;;
commit)
	git commit -m "feat: reopen durable spillable arrangements [skip ci]"
	;;
push)
	git push origin HEAD:master
	;;
status)
	git status --short --branch
	;;
*)
	printf 'unknown mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
