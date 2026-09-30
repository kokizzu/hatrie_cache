#!/usr/bin/env bash
set -euo pipefail

test_pattern='TestTT023SelectivePebbleCheckpointRestoreFiltersPartition|TestBackupBundleSelectsKeyPrefixesAndRestores|TestBackupBundleRejectsSelectivePrefixesForPersistentModes|TestBackupBundleRestoresSelectedPartitionSubset|TestPartitionLocalBackupRejectsIncrementalMode|TestRestoreBackupBundleSupportsPartitionSubsetForPebbleCheckpoint'

case "${1:-test}" in
format)
    gofmt -w \
        hat/hatCache/backup_bundle.go \
        hat/hatCache/backup_bundle_test.go \
        hat/hatCache/backup_pebble_partition_restore.go \
        hat/hatCache/backup_partition_restore_test.go \
        hat/hatCache/backup_restore.go \
        hat/hatCache/tt023_selective_persistent_restore_test.go
	;;
test)
    go test ./hat/hatCache -run "$test_pattern" -count=1
    ;;
race)
    go test -race ./hat/hatCache -run "$test_pattern" -count=1
    ;;
vet)
    go vet ./hat/hatCache
    ;;
benchmark)
    output_file="$(mktemp "${TMPDIR:-/tmp}/hatrie-tt023-benchmark.XXXXXX")"
    trap 'rm -f "$output_file"' EXIT
    go test ./hat/hatCache -run '^$' -bench '^BenchmarkTT023(Selective|Full)PebbleCheckpointRestore$' -benchmem -count=5 >"$output_file" 2>&1
    awk 'index($0, "ns/op") > 0 || $0 == "PASS" || index($0, "ok  ") == 1 { print }' "$output_file"
    ;;
package)
    go test ./hat/hatCache -count=1
    ;;
status)
    git diff --check
    git status --short
    git diff --stat -- Makefile scripts/tt023-selective-persistent-restore.sh hat/hatCache/backup_bundle.go hat/hatCache/backup_bundle_test.go hat/hatCache/backup_pebble_partition_restore.go hat/hatCache/backup_partition_restore_test.go hat/hatCache/backup_restore.go hat/hatCache/tt023_selective_persistent_restore_test.go ADOPTED_QUERY_ENGINE_IDEAS.md ENGINE_IDEAS.md BENCHMARK.md
    ;;
commit)
    git add Makefile scripts/tt023-selective-persistent-restore.sh hat/hatCache/backup_bundle.go hat/hatCache/backup_bundle_test.go hat/hatCache/backup_pebble_partition_restore.go hat/hatCache/backup_partition_restore_test.go hat/hatCache/backup_restore.go hat/hatCache/tt023_selective_persistent_restore_test.go ADOPTED_QUERY_ENGINE_IDEAS.md ENGINE_IDEAS.md BENCHMARK.md
    git commit -m 'feat(backup): restore selected Pebble partitions [skip ci]'
    ;;
push)
    git push origin HEAD
    ;;
*)
	printf 'usage: %s format|test|race|vet|benchmark|package|status|commit|push\n' "$0" >&2
	exit 2
	;;
esac
