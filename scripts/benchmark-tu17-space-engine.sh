#!/usr/bin/env bash
set -euo pipefail

count="${COUNT:-5}"
case "$count" in
  ''|*[!0-9]*)
    printf 'COUNT must be a non-negative integer\n' >&2
    exit 2
    ;;
esac

go test ./hat/hatSpace -run '^$' -bench '^BenchmarkSpaceEngine' -benchmem -count="$count"
