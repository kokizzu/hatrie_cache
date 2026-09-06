#!/usr/bin/env bash
set -euo pipefail

tmpdir=$(mktemp -d "$PWD/.bench-tmp.XXXXXX")
cleanup() {
	rm -rf -- "$tmpdir"
}
trap cleanup EXIT

TMPDIR="$tmpdir" go test ./hat/hatMetrics -run '^$' -bench '^BenchmarkOperatorMemoryRegistrySnapshot$' -benchmem -count=5
