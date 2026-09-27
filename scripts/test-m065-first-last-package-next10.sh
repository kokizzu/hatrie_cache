#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m065-first-last-package.XXXXXX")
output_file=$(mktemp "${TMPDIR:-/tmp}/hatrie-m065-first-last-package-output.XXXXXX")
trap 'chmod -R u+w "$cache_dir" 2>/dev/null || true; rm -rf "$cache_dir" "$output_file"' EXIT
export GOCACHE="$cache_dir/go-build"
if go test ./hat/hatSql >"$output_file" 2>&1; then
  cat "$output_file"
else
  status=$?
  cat "$output_file"
  exit "$status"
fi
