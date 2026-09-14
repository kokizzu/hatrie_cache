#!/usr/bin/env bash
set -euo pipefail

commit_message="${1:-feat(hatSql): add incremental differential join}"
feature_paths=(
  MZ030_INCREMENTAL_JOIN.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  README.md
  hat/hatSql/m030_incremental_join.go
  hat/hatSql/m030_incremental_join_test.go
  hat/hatSql/m030_incremental_join_benchmark_test.go
  scripts/test-mz030-incremental-join.sh
  scripts/benchmark-mz030-incremental-join.sh
  scripts/format-mz030-incremental-join.sh
  scripts/race-mz030-incremental-join.sh
  scripts/vet-mz030-incremental-join.sh
  scripts/review-mz030-incremental-join.sh
  scripts/verify-mz030-incremental-join.sh
  scripts/commit-mz030-incremental-join.sh
  scripts/push-mz030-incremental-join.sh
)
expected_feature_paths=("${feature_paths[@]}" Makefile)

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing to mix MZ-030 with pre-existing staged changes' >&2
  exit 1
fi

makefile_block=(
  ''
  '.PHONY: test-mz030-incremental-join benchmark-mz030-incremental-join format-mz030-incremental-join race-mz030-incremental-join vet-mz030-incremental-join review-mz030-incremental-join verify-mz030-incremental-join commit-mz030-incremental-join push-mz030-incremental-join'
  'test-mz030-incremental-join:'
  $'\t@bash scripts/test-mz030-incremental-join.sh'
  'benchmark-mz030-incremental-join:'
  $'\t@bash scripts/benchmark-mz030-incremental-join.sh'
  'format-mz030-incremental-join:'
  $'\t@bash scripts/format-mz030-incremental-join.sh'
  'race-mz030-incremental-join:'
  $'\t@bash scripts/race-mz030-incremental-join.sh'
  'vet-mz030-incremental-join:'
  $'\t@bash scripts/vet-mz030-incremental-join.sh'
  'review-mz030-incremental-join:'
  $'\t@bash scripts/review-mz030-incremental-join.sh'
  'verify-mz030-incremental-join:'
  $'\t@bash scripts/verify-mz030-incremental-join.sh'
  ''
  'commit-mz030-incremental-join:'
  $'\t@bash scripts/commit-mz030-incremental-join.sh'
  ''
  'push-mz030-incremental-join:'
  $'\t@bash scripts/push-mz030-incremental-join.sh'
)

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
git show HEAD:Makefile > "$tmp_dir/base-Makefile"
printf '%s\n' "${makefile_block[@]}" > "$tmp_dir/MZ030-Makefile-block"
base_lines="$(wc -l < "$tmp_dir/base-Makefile")"
block_lines="$(wc -l < "$tmp_dir/MZ030-Makefile-block")"
{
  printf '%s\n' 'diff --git a/Makefile b/Makefile'
  printf '%s\n' '--- a/Makefile'
  printf '%s\n' '+++ b/Makefile'
  printf '@@ -%s,0 +%s,%s @@\n' "$base_lines" "$base_lines" "$block_lines"
  while IFS= read -r line || [[ -n "$line" ]]; do
    printf '+%s\n' "$line"
  done < "$tmp_dir/MZ030-Makefile-block"
} > "$tmp_dir/MZ030-Makefile.patch"

git add -- "${feature_paths[@]}"
git apply --cached "$tmp_dir/MZ030-Makefile.patch"

staged_paths="$(git diff --cached --name-only)"
expected_paths="$(printf '%s\n' "${expected_feature_paths[@]}" | sort)"
if [[ "$staged_paths" != "$expected_paths" ]]; then
  printf '%s\n' 'staged path set does not match the MZ-030 feature' >&2
  printf '%s\n' 'staged:' "$staged_paths" >&2
  printf '%s\n' 'expected:' "$expected_paths" >&2
  exit 1
fi
git diff --cached --check
git commit -m "$commit_message"
