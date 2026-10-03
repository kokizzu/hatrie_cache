#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_dir"

temporary_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-t033-benchmark.XXXXXX")
trap 'rm -rf "$temporary_dir"' EXIT

run_benchmark() {
	local pattern=$1
	GOTMPDIR="$temporary_dir" go test -run '^$' -bench "$pattern" -benchmem -count=5 ./hat/hatAuth
}

case "${1:-all}" in
	baseline)
		run_benchmark '^BenchmarkT33Before'
		;;
	after)
		run_benchmark '^BenchmarkT33After'
		;;
	all)
		run_benchmark '^BenchmarkT33Before'
		run_benchmark '^BenchmarkT33After'
		;;
	*)
		printf '%s\n' 'usage: baseline|after|all'
		exit 2
		;;
esac
