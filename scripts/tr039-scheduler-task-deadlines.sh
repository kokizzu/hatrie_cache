#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_root="${TMPDIR:-/tmp}/hatrie-tr039-${BASHPID}"
mkdir -p "$tmp_root/gocache" "$tmp_root/gotmp"
cleanup() {
	rm -rf "$tmp_root"
}
trap cleanup EXIT

run_go() {
	GOCACHE="$tmp_root/gocache" GOTMPDIR="$tmp_root/gotmp" go "$@"
}

case "${1:-}" in
	format)
		gofmt -w \
			"$root/hat/hatPipeline/task_options.go" \
			"$root/hat/hatPipeline/scheduler.go" \
			"$root/hat/hatPipeline/scheduler_test.go" \
			"$root/hat/hatPipeline/mz020_resizable_scheduler.go" \
			"$root/hat/hatPipeline/mz020_resizable_scheduler_test.go" \
			"$root/hat/hatPipeline/mz020_resizable_scheduler_benchmark_test.go"
		;;
	test)
		run_go test ./hat/hatPipeline
		;;
	full)
		run_go test ./...
		;;
	race)
		run_go test -race ./hat/hatPipeline
		;;
	vet)
		run_go vet ./hat/hatPipeline
		;;
	benchmark)
		run_go test ./hat/hatPipeline -run '^$' -bench 'Benchmark(FixedScheduler(NoopBatch|DeadlineNoopBatch))$' -benchmem -count=5 -benchtime=1s
		;;
	*)
		printf 'usage: %s {format|test|full|race|vet|benchmark}\n' "$0" >&2
		exit 2
		;;
esac
