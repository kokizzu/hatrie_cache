#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
test)
  go test ./hat/hatCache -run '^TestTU05SQLTransactionSession' -count=1
  ;;
package)
  go test ./hat/hatCache -count=1
  ;;
race)
  temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-t-u05-race.XXXXXX")"
  trap 'rm -rf "$temp_dir"' EXIT
  mkdir -p "$temp_dir/gocache" "$temp_dir/gotmp"
  GOCACHE="$temp_dir/gocache" GOTMPDIR="$temp_dir/gotmp" go test -race ./hat/hatCache -run '^TestTU05SQLTransactionSession' -count=1
  ;;
vet)
  go vet ./hat/hatCache
  ;;
format)
  gofmt -w hat/hatCache/sql_transaction_session.go hat/hatCache/t_u05_sql_transaction_session_test.go hat/hatCache/t_u05_sql_transaction_session_benchmark_test.go
  ;;
benchmark)
  go test ./hat/hatCache -run '^$' -bench '^BenchmarkTU05' -benchmem -count=5
  ;;
*)
  printf 'usage: %s test|package|race|vet|format|benchmark\n' "$0" >&2
  exit 2
  ;;
esac
