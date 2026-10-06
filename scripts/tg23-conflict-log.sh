#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
baseline)
	go test ./hat/hatReplication -run '^$' -bench '^BenchmarkConflictPolicyResolution$' -benchmem -count=3
	;;
format)
	gofmt -w hat/hatReplication/conflict_event_log.go hat/hatReplication/conflict_event_log_test.go hat/hatReplication/conflict_event_log_benchmark_test.go
	;;
test)
	go test ./hat/hatReplication -run '^TestConflictEventLog' -count=1
	;;
package)
	go test ./hat/hatReplication -count=1
	;;
benchmark)
	go test ./hat/hatReplication -run '^$' -bench '^BenchmarkConflictEventLog' -benchmem -count=5
	;;
race)
	go test -race ./hat/hatReplication -run '^TestConflictEventLog' -count=1
	;;
vet)
	go vet ./hat/hatReplication
	;;
status)
	git status --short
	;;
diff-check)
	git diff --check
	;;
stage)
	git add -- Makefile BENCHMARK.md PRODUCT_IDEA_GAPS.md README.md TU38_CONFLICT_INTROSPECTION.md hat/hatReplication/conflict_event_log.go hat/hatReplication/conflict_event_log_benchmark_test.go hat/hatReplication/conflict_event_log_test.go scripts/tg23-conflict-log.sh
	git diff --cached --check
	;;
commit)
	git diff --cached --check
	git commit -m "feat: add bounded conflict introspection log [skip ci]"
	;;
push)
	git push -u origin codex/inspiration-tg23-next-20261007
	;;
*)
	echo "usage: $0 {baseline|format|test|package|benchmark|race|vet|status|diff-check|stage|commit|push}" >&2
	exit 2
	;;
esac
