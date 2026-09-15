#!/usr/bin/env bash
set -euo pipefail

missing_file="hat/hatSql/asof_join.go"
restored_missing_file=0
if [[ ! -e "$missing_file" ]]; then
  git show "HEAD:$missing_file" > "$missing_file"
  restored_missing_file=1
fi
cleanup() {
  if (( restored_missing_file )); then
    rm -f "$missing_file"
  fi
}
trap cleanup EXIT

mode=${1:-test}
case "$mode" in
test)
	go test ./hat/hatCache -run '^(TestBackupBundleRestoresSelectedPartitionSubset|TestRestoreBackupBundleRejectsPartitionSubsetWithJournal|TestRestoreBackupBundleRejectsPartitionSubsetForPebbleCheckpoint|TestValidatePartitionRestoreSelectionRejectsMismatchedCoveragePair|TestValidatePartitionRestoreSelectionRejectsEmptyBackupMetadata|TestRehearseRestoreRejectsPartitionSubset)$' -count=1
	;;
benchmark)
	go test ./hat/hatCache -run '^$' -bench 'BenchmarkBackupBundleRestorePartitionSelection' -benchmem -benchtime=5x -count=1
	;;
package)
	go test ./hat/hatCache ./hat/hatBackup -count=1
	;;
race)
	go test -race ./hat/hatCache ./hat/hatBackup -run '^(TestBackupBundleRestoresSelectedPartitionSubset|TestRestoreBackupBundleRejectsPartitionSubsetWithJournal|TestRestoreBackupBundleRejectsPartitionSubsetForPebbleCheckpoint|TestValidatePartitionRestoreSelectionRejectsMismatchedCoveragePair|TestValidatePartitionRestoreSelectionRejectsEmptyBackupMetadata|TestRehearseRestoreRejectsPartitionSubset)$' -count=1
	;;
cli)
	go test ./cmd/hatrie-cli -run '^TestRunRestoreBundlePartitionSubsetRestoresOnlySelectedRegion$' -count=1
	;;
vet)
	go vet ./hat/hatCache ./hat/hatBackup
	;;
*)
	echo "usage: $0 [test|benchmark|package|race|cli|vet]" >&2
	exit 2
	;;
esac
