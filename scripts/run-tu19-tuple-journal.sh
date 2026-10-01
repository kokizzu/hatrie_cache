#!/usr/bin/env bash
set -euo pipefail

mode="${1:-}"

case "$mode" in
  format)
    files=(
      hat/hatDataStructure/tu19_tuple_field_update_journal.go
      hat/hatDataStructure/tu19_tuple_field_update_journal_test.go
      hat/hatDataStructure/tu19_tuple_field_update_journal_benchmark_test.go
    )
    existing=()
    for file in "${files[@]}"; do
      if [[ -f "$file" ]]; then
        existing+=("$file")
      fi
    done
    if ((${#existing[@]} > 0)); then
      gofmt -w "${existing[@]}"
    fi
    ;;
  benchmark-baseline)
    go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTUG19TupleFieldUpdateJSON' -benchmem -count=5
    ;;
  benchmark)
    go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTUG19TupleFieldUpdate' -benchmem -count=5
    ;;
  test)
    go test ./hat/hatDataStructure -run '^TestTUG19TupleFieldUpdateJournal' -count=1
    ;;
  race)
    go test -race ./hat/hatDataStructure -run '^TestTUG19TupleFieldUpdateJournal' -count=1
    ;;
  vet)
    go vet ./hat/hatDataStructure
    ;;
  *)
    printf 'usage: %s {format|benchmark-baseline|benchmark|test|race|vet}\n' "$0" >&2
    exit 2
    ;;
esac
