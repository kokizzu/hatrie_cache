#!/usr/bin/env bash
set -euo pipefail

gocache=$(mktemp -d /tmp/hatrie-m052z-go-cache.XXXXXX)
trap 'rm -rf -- "$gocache"' EXIT

case "${1:-}" in
  format)
    gofmt -w \
      hat/hatSql/m052c_native_dataflow.go \
      hat/hatSql/m052p_auto_native_dataflow.go \
      hat/hatSql/m052r_auto_native_ordered_test.go \
      hat/hatSql/m052z_auto_native_unbounded_order_test.go
    ;;
  package)
    GOCACHE="$gocache" go test ./hat/hatSql -count=1
    ;;
  test)
    GOCACHE="$gocache" go test ./hat/hatSql -run '^TestM052zAutomaticNativeUnboundedOrder$' -count=1
    ;;
  benchmark)
    GOCACHE="$gocache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkM052zUnboundedOrder$' -benchmem -count=5
    ;;
  race)
    GOCACHE="$gocache" go test -race ./hat/hatSql -run '^TestM052zAutomaticNativeUnboundedOrder$' -count=1
    ;;
  vet)
    GOCACHE="$gocache" go vet ./hat/hatSql
    ;;
  *)
    printf 'usage: %s format|package|test|benchmark|race|vet\n' "$0" >&2
    exit 2
    ;;
esac
