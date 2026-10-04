#!/usr/bin/env bash
set -euo pipefail

temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-mu036-benchmark.XXXXXX")"
cleanup() {
	rm -rf -- "$temporary_root"
}
trap cleanup EXIT

GOCACHE="$temporary_root/go-cache" go test ./hat/hatSql \
	-run '^$' \
	-bench '^BenchmarkMU036' \
	-benchmem \
	-count="${BENCHMARK_COUNT:-5}"
