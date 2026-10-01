#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  baseline)
    go test ./hat/hatTopology -count=1
    ;;
  feature)
    go test ./hat/hatTopology -run 'TestTU13' -count=1
    ;;
  benchmark-baseline)
    go test ./hat/hatTopology -run '^$' -bench 'BenchmarkTU13BaselineInMemoryMembershipUpdate' -benchmem -count=5
    ;;
  benchmark-feature)
    go test ./hat/hatTopology -run '^$' -bench 'BenchmarkTU13DurableMembershipJoin' -benchmem -benchtime=10x -count=5
    ;;
  race)
    go test -race ./hat/hatTopology -count=1
    ;;
  vet)
    go vet ./hat/hatTopology
    ;;
  *)
    printf 'usage: %s baseline|feature|benchmark-baseline|benchmark-feature|race|vet\n' "$0" >&2
    exit 2
    ;;
esac
