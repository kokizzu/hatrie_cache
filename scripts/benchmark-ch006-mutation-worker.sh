#!/usr/bin/env bash
set -euo pipefail

tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-ch006-worker-benchmark.XXXXXX")"
trap 'rm -rf "$tmp_root"' EXIT
mkdir -p "$tmp_root/gocache" "$tmp_root/gotmp"
run_benchmark() {
	local pattern="$1"
	GOCACHE="$tmp_root/gocache" GOTMPDIR="$tmp_root/gotmp" TMPDIR="$tmp_root" \
		go test ./hat/hatSql -run '^$' -bench "$pattern" -benchmem -count=9 -benchtime=1x
}

# Separate processes and reverse order reduce cross-benchmark fsync queue
# contention; the task workload remains identical in both benchmarks.
run_benchmark '^BenchmarkSQLMutationDependencyQueueRunReady$'
run_benchmark '^BenchmarkSQLMutationDependencyQueueManualProcessReady$'
