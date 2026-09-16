#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
  printf '%s\n' 'Refusing to stage: the index already contains changes.' >&2
  exit 1
fi

paths=(
  README.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  TT011_POINT_IN_TIME_RESTORE.md
  hat/hatBackup/reports.go
  hat/hatCache/backup_partition_restore.go
  hat/hatCache/backup_partition_restore_test.go
  hat/hatCache/backup_point_in_time_restore.go
  hat/hatCache/backup_restore.go
  hat/hatCache/tt011_point_in_time_restore_test.go
  hat/hatCache/tt011_restore_benchmark_test.go
  hat/hatCache/tt011_restore_pit_benchmark_test.go
  cmd/hatrie-cli/main.go
  cmd/hatrie-cli/tt011_point_in_time_restore_test.go
  scripts/test-tt011-point-in-time-restore-c291.sh
  scripts/format-tt011-point-in-time-restore-c291.sh
  scripts/verify-tt011-point-in-time-restore-c291.sh
  scripts/benchmark-before-tt011-point-in-time-restore-c291.sh
  scripts/benchmark-tt011-point-in-time-restore-c291.sh
  scripts/review-tt011-point-in-time-restore-c291.sh
  scripts/stage-tt011-point-in-time-restore-c291.sh
  scripts/inspect-staged-tt011-point-in-time-restore-c291.sh
  scripts/commit-tt011-point-in-time-restore-c291.sh
  scripts/push-tt011-point-in-time-restore-c291.sh
)

for path in "${paths[@]}"; do
  if [[ ! -e "$path" ]]; then
    printf 'Missing feature path: %s\n' "$path" >&2
    exit 1
  fi
done
git add -- "${paths[@]}"

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
git show HEAD:Makefile > "$tmp_dir/Makefile.old"
git show HEAD:Makefile > "$tmp_dir/Makefile.new"
printf '%s\n' \
  '.PHONY: test-tt011-point-in-time-restore-c291' \
  'test-tt011-point-in-time-restore-c291:' \
  $'\t@bash scripts/test-tt011-point-in-time-restore-c291.sh' \
  '' \
  '.PHONY: format-tt011-point-in-time-restore-c291' \
  'format-tt011-point-in-time-restore-c291:' \
  $'\t@bash scripts/format-tt011-point-in-time-restore-c291.sh' \
  '' \
  '.PHONY: verify-tt011-point-in-time-restore-c291' \
  'verify-tt011-point-in-time-restore-c291:' \
  $'\t@bash scripts/verify-tt011-point-in-time-restore-c291.sh' \
  '' \
  '.PHONY: benchmark-before-tt011-point-in-time-restore-c291' \
  'benchmark-before-tt011-point-in-time-restore-c291:' \
  $'\t@bash scripts/benchmark-before-tt011-point-in-time-restore-c291.sh' \
  '' \
  '.PHONY: benchmark-tt011-point-in-time-restore-c291' \
  'benchmark-tt011-point-in-time-restore-c291:' \
  $'\t@bash scripts/benchmark-tt011-point-in-time-restore-c291.sh' \
  '' \
  '.PHONY: review-tt011-point-in-time-restore-c291' \
  'review-tt011-point-in-time-restore-c291:' \
  $'\t@bash scripts/review-tt011-point-in-time-restore-c291.sh' \
  '' \
  '.PHONY: stage-tt011-point-in-time-restore-c291' \
  'stage-tt011-point-in-time-restore-c291:' \
  $'\t@bash scripts/stage-tt011-point-in-time-restore-c291.sh' \
  '' \
  '.PHONY: inspect-staged-tt011-point-in-time-restore-c291' \
  'inspect-staged-tt011-point-in-time-restore-c291:' \
  $'\t@bash scripts/inspect-staged-tt011-point-in-time-restore-c291.sh' \
  '' \
  '.PHONY: commit-tt011-point-in-time-restore-c291' \
  'commit-tt011-point-in-time-restore-c291:' \
  $'\t@bash scripts/commit-tt011-point-in-time-restore-c291.sh' \
  '' \
  '.PHONY: push-tt011-point-in-time-restore-c291' \
  'push-tt011-point-in-time-restore-c291:' \
  $'\t@bash scripts/push-tt011-point-in-time-restore-c291.sh' \
  >> "$tmp_dir/Makefile.new"
set +e
(cd "$tmp_dir" && git diff --no-index -- Makefile.old Makefile.new > Makefile.patch)
diff_status=$?
set -e
if [[ "$diff_status" -ne 1 ]]; then
  printf 'Unexpected Makefile patch generation status: %s\n' "$diff_status" >&2
  exit 1
fi
sed -e 's|a/Makefile.old|a/Makefile|g' -e 's|b/Makefile.new|b/Makefile|g' \
  "$tmp_dir/Makefile.patch" > "$tmp_dir/Makefile.rebased.patch"
git apply --cached "$tmp_dir/Makefile.rebased.patch"
