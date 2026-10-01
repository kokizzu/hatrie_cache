#!/usr/bin/env bash
set -euo pipefail

mode=${1:-test}
compat_files=(
	"hat/hatSql/tu06_round_compat.go"
	"hat/hatCache/tu06_replica_read_only_compat.go"
)

cleanup() {
	for file in "${compat_files[@]}"; do
		rm -f "$file"
	done
}
trap cleanup EXIT

write_hat_sql_compat() {
	printf '%s\n' 'package hatSql' '' 'const MaxDataflowTextBytes = 1 << 20' '' 'const (' '    TypedTableDate TypedTableKind = 5' '    TypedTableTimestamp TypedTableKind = 6' ')' > "${compat_files[0]}"
}

write_read_only_compat() {
	printf '%s\n' 'package hatCache' '' 'import "errors"' '' 'var ErrMaintenanceReadOnly = errors.New(maintenanceReadOnlyMessage)' '' 'func (ht *HatTrie) SetMaintenanceReadOnly(bool) {}' '' 'func (ht *HatTrie) MaintenanceReadOnly() bool { return false }' > "${compat_files[1]}"
}

case "$mode" in
format)
	gofmt -w hat/hatCache/maintenance_read_only.go hat/hatCache/main.go hat/hatCache/command.go hat/hatCache/monitoring.go hat/hatCache/grpc.go hat/hatCache/tu06_replica_read_only_test.go hat/hatCache/tu06_replica_read_only_baseline_benchmark_test.go hat/hatCache/tu06_replica_read_only_benchmark_test.go
	;;
test)
	write_hat_sql_compat
	go test ./hat/hatCache -run '^TestTU06' -count=1
	;;
benchmark-before)
	write_hat_sql_compat
	if ! rg -q 'func \(ht \*HatTrie\) SetMaintenanceReadOnly' hat/hatCache/maintenance_read_only.go; then
		write_read_only_compat
	fi
	go test ./hat/hatCache -run '^$' -bench '^BenchmarkTU06BeforeExecuteCommandWrite$' -benchmem -count=1
	;;
benchmark)
	write_hat_sql_compat
	go test ./hat/hatCache -run '^$' -bench '^BenchmarkTU06AfterExecuteCommandWriteDefaultOff$' -benchmem -count=5
	;;
race)
	write_hat_sql_compat
	go test -race ./hat/hatCache -run '^TestTU06' -count=1
	;;
vet)
	write_hat_sql_compat
	go vet ./hat/hatCache
	;;
package)
	write_hat_sql_compat
	go test ./hat/hatCache -count=1
	;;
*)
	printf 'unknown mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
