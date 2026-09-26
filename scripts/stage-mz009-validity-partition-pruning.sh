#!/usr/bin/env bash
set -euo pipefail

feature_files=(
  README.md
  BENCHMARK.md
  ENGINE_IDEAS.md
  SQL_TEMPORAL_VALIDITY.md
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
  MZ009_VALIDITY_PARTITION_PRUNING.md
  hat/hatSql/contracts.go
  hat/hatSql/query.go
  hat/hatSql/mz009_validity_partition_pruning_test.go
  scripts/test-mz009-validity-partition-pruning.sh
  scripts/benchmark-mz009-validity-partition-pruning.sh
  scripts/format-mz009-validity-partition-pruning.sh
  scripts/test-mz009-validity-partition-pruning-package.sh
  scripts/race-mz009-validity-partition-pruning.sh
  scripts/vet-mz009-validity-partition-pruning.sh
  scripts/verify-mz009-validity-partition-pruning-docs.sh
  scripts/review-mz009-validity-partition-pruning.sh
  scripts/stage-mz009-validity-partition-pruning.sh
  scripts/commit-mz009-validity-partition-pruning.sh
  scripts/push-mz009-validity-partition-pruning.sh
)

if test -n "$(git diff --cached --name-only)"; then
  echo "refusing to stage MZ-009 with pre-existing staged paths" >&2
  git diff --cached --name-only >&2
  exit 1
fi

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-mz009-stage.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT
base="$tmp_dir/Makefile.base"
candidate="$tmp_dir/Makefile.candidate"
block="$tmp_dir/Makefile.block"
patch_file="$tmp_dir/Makefile.patch"

git show HEAD:Makefile > "$base"
awk '
  /^# MZ009_VALIDITY_PARTITION_PRUNING_FEATURE_TARGETS_BEGIN$/ { capture = 1 }
  capture { print }
  capture && /^# MZ009_VALIDITY_PARTITION_PRUNING_FEATURE_TARGETS_END$/ { exit }
' Makefile > "$block"
test -s "$block"
if grep -q '^# MZ009_VALIDITY_PARTITION_PRUNING_FEATURE_TARGETS_BEGIN$' "$base"; then
  echo "MZ-009 target block already exists in HEAD" >&2
  exit 1
fi

awk -v block="$block" '
  { print }
  /^# CH037_COLUMNAR_ARRAY_JOIN_FEATURE_TARGETS_END$/ {
    while ((getline line < block) > 0) print line
    close(block)
  }
' "$base" > "$candidate"

diff_status=0
diff -u --label a/Makefile --label b/Makefile "$base" "$candidate" > "$patch_file" || diff_status=$?
test "$diff_status" = 1
git apply --cached "$patch_file"
git add -- "${feature_files[@]}"
git diff --cached --check
git diff --cached --name-only
