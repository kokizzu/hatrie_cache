#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_root="${TMPDIR:-/tmp}/hatrie-chg38-${BASHPID}"
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
		gofmt -w "$root/hat/hatMappedPart"/*chg38_mmap_readonly*.go
		;;
	baseline)
		run_go test ./hat/hatMappedPart -run '^$' -bench '^BenchmarkMappedReadOnlyPartBaselineReadFile$' -benchmem -count=5 -benchtime=1s
		;;
	test)
		run_go test ./hat/hatMappedPart -run 'MappedReadOnlyPart'
		;;
	race)
		run_go test -race ./hat/hatMappedPart -run 'MappedReadOnlyPart'
		;;
	vet)
		run_go vet ./hat/hatMappedPart
		;;
	benchmark)
		run_go test ./hat/hatMappedPart -run '^$' -bench '^BenchmarkMappedReadOnlyPart' -benchmem -count=5 -benchtime=1s
		;;
	docs)
		rg -n -C 2 'C038|mmap|read-only|read.only' "$root/README.md" "$root/INSPIRATION.md" "$root/BENCHMARK.md" || true
		;;
	*)
		printf 'usage: %s {format|baseline|test|race|vet|benchmark|docs}\n' "$0" >&2
		exit 2
		;;
esac
