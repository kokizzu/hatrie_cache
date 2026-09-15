#!/usr/bin/env bash
set -euo pipefail

expected=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  ENGINE_IDEAS.md
  Makefile
  README.md
  cmd/hatrie-cli/partition_local_backup_test.go
  hat/hatBackup/model.go
  hat/hatCache/backup_partition_restore.go
  hat/hatCache/backup_partition_restore_benchmark_test.go
  hat/hatCache/backup_partition_restore_test.go
  hat/hatCache/backup_restore.go
  hat/hatCache/backup_selective_test.go
  scripts/check-selective-restore-c224.sh
  scripts/commit-selective-restore-c224.sh
  scripts/format-selective-restore-c224.sh
  scripts/push-selective-restore-c224.sh
  scripts/run-selective-restore-c224.sh
  scripts/stage-selective-restore-c224.sh
)
mapfile -t actual < <(git diff --cached --name-only)
if [[ "${actual[*]}" != "${expected[*]}" ]]; then
	echo "staged paths do not match the C224 feature" >&2
	printf '%s\n' "${actual[@]}" >&2
	exit 1
fi

git diff --cached --check
git commit -m "Add selective partition restore"
