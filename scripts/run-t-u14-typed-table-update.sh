#!/usr/bin/env bash
set -euo pipefail

mode="${1:-}"

case "$mode" in
  format)
    files=(
      hat/hatSql/t_u14_typed_table_update_benchmark_test.go
      hat/hatSql/t_u14_typed_table_update_test.go
      hat/hatSql/typed_table_updates.go
      hat/hatSql/round27_compat.go
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
    go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableFieldUpdateReadUpsertBaseline$' -benchmem -count=5
    ;;
  benchmark)
    go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableFieldUpdate' -benchmem -count=5
    ;;
  test)
    go test ./hat/hatSql -run '^TestTypedTableFieldUpdate' -count=1
    ;;
  race)
    go test -race ./hat/hatSql -run '^TestTypedTableFieldUpdate' -count=1
    ;;
  vet)
    go vet ./hat/hatSql
    ;;
  package)
    go test ./hat/hatSql -count=1
    ;;
  all)
    go test ./... -count=1
    ;;
  *)
    printf 'usage: %s {format|benchmark-baseline|benchmark|test|race|vet|package|all}\n' "$0" >&2
    exit 2
    ;;
esac
