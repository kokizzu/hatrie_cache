#!/usr/bin/env bash
set -euo pipefail

mode=${1:-final}
case "$mode" in
  baseline)
    pattern='BenchmarkCH014BExistingClaimReadyNoReady$'
    ;;
  final)
    pattern='BenchmarkCH014B'
    ;;
  *)
    printf 'usage: %s [baseline|final]\n' "$0" >&2
    exit 2
    ;;
esac

go test ./hat/hatSql -run '^$' -bench "$pattern" -benchmem -count=5 -cpu=1
