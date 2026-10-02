#!/usr/bin/env bash
set -euo pipefail

mode="${1:-unit}"
tmp_dir="$(mktemp -d -p /tmp hatrie-cache-chg19-test-XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT
export GOCACHE="$tmp_dir/go-cache"

case "$mode" in
format)
	gofmt -w \
		hat/hatJournal/journal.go \
		hat/hatJournal/sync_policy.go \
		hat/hatJournal/sync_policy_test.go \
		hat/hatCache/journal.go \
		hat/hatCache/journal_durability.go \
		hat/hatCache/journal_sync_policy.go \
		hat/hatCache/monitoring.go \
		hat/hatCache/t_u34_per_space_wal_test.go \
		hat/hatCache/t_u34_per_space_wal_benchmark_test.go
	;;
unit)
	go test ./hat/hatJournal ./hat/hatCache -run 'TestSyncPolicy|TestTU34' -count=1
	;;
race)
	go test -race ./hat/hatJournal ./hat/hatCache -run 'TestSyncPolicy|TestTU34' -count=1
	;;
vet)
	go vet ./hat/hatJournal ./hat/hatCache
	;;
package)
	go test ./hat/hatJournal ./hat/hatCache -count=1
	;;
package-skip-baseline)
	go test ./hat/hatJournal ./hat/hatCache -skip 'TestHatTrieSQLJSONIndexValueEstimateUsesExactPostingCount|TestHatTrieSQLJSONIndexStatsExposeSkewAndExplainEstimate' -count=1
	;;
benchmark)
	go test ./hat/hatCache -run '^$' -bench '^BenchmarkTU34' -benchtime=100ms -count=1
	;;
benchmark-disk)
	go test ./hat/hatCache -run '^$' -bench '^BenchmarkTU34.*Disk$' -benchtime=100ms -count=1
	;;
*)
	printf 'unknown chg19 WAL test mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
