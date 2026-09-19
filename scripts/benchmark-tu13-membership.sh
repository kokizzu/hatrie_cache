#!/usr/bin/env bash
set -euo pipefail

mode=${1:-candidate}
case "$mode" in
  candidate)
    go test ./hat/hatTopology -run '^$' -bench '^BenchmarkTU13Membership' -benchmem -count=5
    ;;
  baseline)
    go test ./hat/hatTopology -tags=tu13baseline -run '^$' -bench '^BenchmarkTU13BaselineTopologyClone$' -benchmem -count=5
    ;;
  *)
    printf 'usage: %s {candidate|baseline}\n' "$0" >&2
    exit 2
    ;;
esac
