#!/usr/bin/env bash
set -euo pipefail

mode="${1:-}"
case "${mode}" in
    baseline)
        go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLTransaction' -benchmem -benchtime=1s -count=3
        ;;
    inspect)
        rg -n '^func Benchmark' hat/hatCache/*transaction*benchmark*.go hat/hatCache/*transaction*.go
        ;;
    inspect-gap)
        rg -n -C 2 'T-U05|session transaction settings|transaction settings' PRODUCT_IDEA_GAPS.md
        ;;
    format)
        gofmt -w hat/hatCache/sql_transaction_session.go hat/hatCache/sql_transaction_session_test.go hat/hatCache/sql_transaction_session_benchmark_test.go
        ;;
    test)
        go test ./hat/hatCache -run '^TestSQLTransactionSession' -count=1
        ;;
    benchmark)
        go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLTransactionSession' -benchmem -benchtime=1s -count=3
        ;;
    verify)
        go test ./hat/hatCache -run '^TestSQLTransaction(Session|Options|Isolation|ReadOnly|Timeout)' -count=1
        go test -race ./hat/hatCache -run '^TestSQLTransactionSession' -count=1
        go vet ./hat/hatCache
        ;;
    package)
        go test ./hat/hatCache -count=1
        ;;
    full)
        cache_dir="$(mktemp -d /tmp/hatrie-cache-chg18-gocache.XXXXXX)"
        trap 'rm -rf "${cache_dir}"' EXIT
        GOCACHE="${cache_dir}" go test ./... -count=1
        ;;
    *)
        printf 'usage: %s baseline|inspect|inspect-gap|format|test|benchmark|verify|package|full\n' "$0" >&2
        exit 2
        ;;
esac
