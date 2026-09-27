#!/usr/bin/env bash
set -euo pipefail
cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m065-first-last-benchmark.XXXXXX")
output_file=$(mktemp "${TMPDIR:-/tmp}/hatrie-m065-first-last-benchmark-output.XXXXXX")
trap 'chmod -R u+w "$cache_dir" 2>/dev/null || true; rm -rf "$cache_dir" "$output_file"' EXIT
export GOCACHE="$cache_dir/go-build"
if go test ./hat/hatSql -run '^$' -bench '^BenchmarkM065FirstLastWindow$' -benchmem -count=5 >"$output_file" 2>&1; then
  grep '^BenchmarkM065FirstLastWindow' "$output_file"
else
  status=$?
  cat "$output_file"
  exit "$status"
fi
