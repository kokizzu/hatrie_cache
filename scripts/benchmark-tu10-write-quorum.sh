#!/usr/bin/env bash
set -euo pipefail

mode=${1:-candidate}
case "$mode" in
  candidate)
    go test ./hat/hatReplication -run '^$' -bench '^BenchmarkTU10JournalWriteQuorum' -benchmem -count=5
    ;;
  baseline)
    go test ./hat/hatReplication -tags=tu10baseline -run '^$' -bench '^BenchmarkTU10BaselineWriteQuorumDecision$' -benchmem -count=5
    ;;
  generic)
    go test ./hat/hatReplication -run '^$' -bench '^BenchmarkExecuteWriteQuorumThreeTargets$' -benchmem -count=5
    ;;
  *)
    printf 'usage: %s {candidate|baseline|generic}\n' "$0" >&2
    exit 2
    ;;
esac
