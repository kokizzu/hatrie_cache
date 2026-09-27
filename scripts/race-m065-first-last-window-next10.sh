#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m065-first-last-race.XXXXXX")
output_file=$(mktemp "${TMPDIR:-/tmp}/hatrie-m065-first-last-race-output.XXXXXX")
trap 'chmod -R u+w "$cache_dir" 2>/dev/null || true; rm -rf "$cache_dir" "$output_file"' EXIT
export GOCACHE="$cache_dir/go-build"
if go test -race ./hat/hatSql -run 'TestM065FirstLastWindow' >"$output_file" 2>&1; then
  cat "$output_file"
else
  status=$?
  cat "$output_file"
  exit "$status"
fi
