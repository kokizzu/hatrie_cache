#!/usr/bin/env bash
set -euo pipefail

cache_dir="$PWD/.round39-go-cache"
tmp_dir="$PWD/.round39-go-tmp"
rm -rf "$cache_dir" "$tmp_dir"
mkdir -p "$cache_dir" "$tmp_dir/go" "$tmp_dir/tmp"
cleanup_round39_test_artifacts() {
  rm -rf "$cache_dir" "$tmp_dir"
}
trap cleanup_round39_test_artifacts EXIT
export GOCACHE="$cache_dir"
export TMPDIR="$tmp_dir/tmp"

case "${1:-feature}" in
    baseline)
      if ! go test -count=1 ./hat/hatCache >"$tmp_dir/baseline.log" 2>&1; then
        rg -n -A 12 -B 5 -- '^(--- FAIL|FAIL)' "$tmp_dir/baseline.log" || true
        exit 1
      fi
      cat "$tmp_dir/baseline.log"
      ;;
    benchmark-baseline)
      go test -v -count=1 ./hat/hatCache -run '^$' -bench '^BenchmarkTU06Baseline' -benchmem -benchtime=1s -count=5
      ;;
    feature)
      go test -v -count=1 ./hat/hatCache -run '^TestTU06'
      ;;
    benchmark-feature)
      go test -v -count=1 ./hat/hatCache -run '^$' -bench '^BenchmarkTU06' -benchmem -benchtime=1s -count=5
      ;;
  race)
    go test -race ./hat/hatCache -run '^TestTU06'
    ;;
  vet)
    go vet ./hat/hatCache
    ;;
  *)
    printf '%s\n' 'usage: test-round39-replica-read-only.sh {baseline|benchmark-baseline|feature|benchmark-feature|race|vet}' >&2
    exit 2
    ;;
esac
