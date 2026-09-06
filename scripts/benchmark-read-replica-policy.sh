#!/usr/bin/env bash
set -euo pipefail

tmpdir=$(mktemp -d "$PWD/.bench-tmp.XXXXXX")
cleanup() {
	rm -rf -- "$tmpdir"
}
trap cleanup EXIT

TMPDIR="$tmpdir" go test ./hat/hatReplication -run '^$' -bench '^BenchmarkSelectReadReplica$' -benchmem -count=5
