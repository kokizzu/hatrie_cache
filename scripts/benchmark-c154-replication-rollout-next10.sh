#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c154-bench.XXXXXX")
output_file=$(mktemp "${TMPDIR:-/tmp}/hatrie-c154-bench-output.XXXXXX")
cleanup() {
	rm -rf "$cache_dir" "$output_file"
}
trap cleanup EXIT

GOCACHE="$cache_dir" go test ./hat/hatCache \
  -run '^$' \
  -bench '^BenchmarkC154ReplicationSchemaRollout$' \
  -benchmem \
  -benchtime=200ms \
  -count=5 >"$output_file"
cat "$output_file"
