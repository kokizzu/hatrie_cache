#!/usr/bin/env bash
set -euo pipefail

mode="${1:-final}"
case "$mode" in
  baseline)
    pattern='BenchmarkMZ047ExistingManagedComputePool$'
    ;;
  final)
    pattern='BenchmarkMZ047'
    ;;
  *)
    printf 'usage: %s {baseline|final}\n' "$0" >&2
    exit 2
    ;;
esac

go test ./hat/hatSql -run '^$' -bench "$pattern" -benchmem -count=5 -cpu=1
