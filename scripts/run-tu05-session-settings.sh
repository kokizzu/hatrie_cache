#!/usr/bin/env bash
set -euo pipefail

compat=hat/hatSql/tu05_round_compat.go

cleanup() {
    rm -f "$compat"
}

run_with_compat() {
    printf '%s\n' 'package hatSql' > "$compat"
    printf '%s\n' 'const MaxDataflowTextBytes = 1 << 20' >> "$compat"
    printf '%s\n' 'const TypedTableDate TypedTableKind = 5' >> "$compat"
    printf '%s\n' 'const TypedTableTimestamp TypedTableKind = 6' >> "$compat"
    trap cleanup EXIT
    "$@"
}

case "${1:-test}" in
format)
    files=(
        hat/hatSql/tu05_session_transaction_settings_test.go
        hat/hatSql/tu05_session_transaction_settings_baseline_benchmark_test.go
        hat/hatSql/tu05_session_transaction_settings_benchmark_test.go
    )
    if [[ -f hat/hatSql/tu05_session_transaction_settings.go ]]; then
        files+=(hat/hatSql/tu05_session_transaction_settings.go)
    fi
    gofmt -w "${files[@]}"
	;;
benchmark-before)
    go test hat/hatSql/tu05_session_transaction_settings_baseline_benchmark_test.go -run '^$' -bench '^BenchmarkTU05Before' -benchmem -count=5
	;;
test)
    run_with_compat go test ./hat/hatSql -run '^TestTU05SessionTransactionSettings' -count=1
	;;
race)
    run_with_compat go test -race ./hat/hatSql -run '^TestTU05SessionTransactionSettings' -count=1
	;;
vet)
    run_with_compat go vet ./hat/hatSql
	;;
benchmark)
    run_with_compat go test ./hat/hatSql -run '^$' -bench '^BenchmarkTU05' -benchmem -count=5
	;;
package)
    run_with_compat go test ./hat/hatSql -count=1
	;;
*)
    printf 'usage: %s {format|benchmark-before|test|race|vet|benchmark|package}\n' "$0" >&2
    exit 2
	;;
esac
