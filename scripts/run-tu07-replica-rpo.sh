#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
compat_file="hat/hatSql/tu07_round_compat.go"
cache_dir=".tu07-go-cache-$$"
cleanup() {
	rm -f "$compat_file"
	rm -rf "$cache_dir"
}
trap cleanup EXIT
export GOTOOLCHAIN=auto
export GOCACHE="$PWD/$cache_dir"
printf '%s\n' 'package hatSql' > "$compat_file"
printf '%s\n' '' 'const MaxDataflowTextBytes = 1 << 20' >> "$compat_file"
printf '%s\n' '' 'const (' 'TypedTableDate TypedTableKind = 5' 'TypedTableTimestamp TypedTableKind = 6' ')' >> "$compat_file"
case "$mode" in
test)
	go test ./hat/hatCache -run '^TestTU07PrometheusReplicationRegionStatus' -count=1 -v
	;;
benchmark)
	go test ./hat/hatCache -run '^$' -bench '^BenchmarkTU07MonitoringMetricsWithRegionalReplication$' -benchtime=200ms -count=5
	;;
package)
	go test ./hat/hatCache -count=1
	;;
race)
	go test -race ./hat/hatCache -run '^TestTU07PrometheusReplicationRegionStatus' -count=1
	;;
vet)
	go vet ./hat/hatCache
	;;
*)
	printf 'usage: %s {test|benchmark|package|race|vet}\n' "$0" >&2
	exit 2
	;;
esac
