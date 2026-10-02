#!/usr/bin/env bash
set -euo pipefail

mode="${1:-unit}"
tmp_dir="$(mktemp -d -p /tmp hatrie-cache-chg20-test-XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT
export GOCACHE="$tmp_dir/gocache"

case "$mode" in
  format)
    gofmt -w hat/hatDataStructure/tuple_field_update_journal.go hat/hatDataStructure/tuple_field_update_journal_test.go hat/hatDataStructure/tuple_field_update_journal_benchmark_test.go
    ;;
  red|unit)
    go test ./hat/hatDataStructure -run 'TestTupleFieldUpdateJournal' -count=1
    ;;
  race)
    go test -race ./hat/hatDataStructure -run 'TestTupleFieldUpdateJournal' -count=1
    ;;
  vet)
    go vet ./hat/hatDataStructure
    ;;
  package)
    go test ./hat/hatDataStructure -count=1
    ;;
  benchmark)
    go test ./hat/hatDataStructure -run '^$' -bench 'TupleFieldUpdateJournal' -benchmem -benchtime=100ms -count=1
    ;;
  *)
    printf 'usage: %s [format|red|unit|race|vet|package|benchmark]\n' "$0" >&2
    exit 2
    ;;
esac
