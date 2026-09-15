#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
package_path="./hat/hatDataStructure"
case "$mode" in
  test)
    go test "$package_path" -run '^TestTDigestAggregateStateRoundTripMergeAndValidation$' -count=1
    ;;
  package)
    go test "$package_path"
    ;;
  race)
    go test -race "$package_path" -run '^TestTDigestAggregateStateRoundTripMergeAndValidation$' -count=1
    ;;
  vet)
    go vet "$package_path"
    ;;
  benchmark)
    go test "$package_path" -run '^$' -bench 'TDigestAggregateState' -benchmem -benchtime="${BENCHTIME:-1s}" -count="${BENCHCOUNT:-5}"
    ;;
  *)
    printf 'unknown mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
