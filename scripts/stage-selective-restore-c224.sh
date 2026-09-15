#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
	echo "refusing to stage with pre-existing index changes" >&2
	exit 1
fi

original_makefile=$(mktemp)
staged_makefile=$(mktemp)
cleanup() {
	cp "$original_makefile" Makefile
	rm -f "$original_makefile" "$staged_makefile"
}
trap cleanup EXIT

cp Makefile "$original_makefile"
git show HEAD:Makefile > "$staged_makefile"
printf '\n# C224 selective partition restore\n\n.PHONY: test-selective-restore-c224 benchmark-selective-restore-c224 test-selective-restore-package-c224 race-selective-restore-c224 test-selective-restore-cli-c224 vet-selective-restore-c224 format-selective-restore-c224 check-selective-restore-c224\n\ntest-selective-restore-c224:\n\tbash ./scripts/run-selective-restore-c224.sh\n\nbenchmark-selective-restore-c224:\n\tbash ./scripts/run-selective-restore-c224.sh benchmark\n\ntest-selective-restore-package-c224:\n\tbash ./scripts/run-selective-restore-c224.sh package\n\nrace-selective-restore-c224:\n\tbash ./scripts/run-selective-restore-c224.sh race\n\ntest-selective-restore-cli-c224:\n\tbash ./scripts/run-selective-restore-c224.sh cli\n\nvet-selective-restore-c224:\n\tbash ./scripts/run-selective-restore-c224.sh vet\n\nformat-selective-restore-c224:\n\tbash ./scripts/format-selective-restore-c224.sh\n\ncheck-selective-restore-c224:\n\tbash ./scripts/check-selective-restore-c224.sh\n' >> "$staged_makefile"
cp "$staged_makefile" Makefile

git add \
  README.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  cmd/hatrie-cli/partition_local_backup_test.go \
  hat/hatBackup/model.go \
  hat/hatCache/backup_partition_restore.go \
  hat/hatCache/backup_partition_restore_benchmark_test.go \
  hat/hatCache/backup_partition_restore_test.go \
  hat/hatCache/backup_restore.go \
  hat/hatCache/backup_selective_test.go \
  Makefile \
  scripts/check-selective-restore-c224.sh \
  scripts/format-selective-restore-c224.sh \
  scripts/run-selective-restore-c224.sh \
  scripts/stage-selective-restore-c224.sh \
  scripts/commit-selective-restore-c224.sh \
  scripts/push-selective-restore-c224.sh

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
	echo "unexpected staged paths" >&2
	printf '%s\n' "${actual[@]}" >&2
	exit 1
fi

git diff --cached --check
printf '%s\n' "${actual[@]}"
