#!/usr/bin/env bash
set -euo pipefail

commit_message="${1:-feat(hatSql): add incremental interval join}"
feature_paths=(
  MZ029_INCREMENTAL_INTERVAL_JOIN.md
  INSPIRATION_BACKLOG.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  README.md
  hat/hatSql/m029_incremental_interval_join.go
  hat/hatSql/m029_incremental_interval_join_test.go
  hat/hatSql/m029_incremental_interval_join_benchmark_test.go
  scripts/test-mz029-incremental-interval-join.sh
  scripts/benchmark-mz029-incremental-interval-join.sh
  scripts/format-mz029-incremental-interval-join.sh
  scripts/race-mz029-incremental-interval-join.sh
  scripts/vet-mz029-incremental-interval-join.sh
  scripts/review-mz029-incremental-interval-join.sh
  scripts/verify-mz029-incremental-interval-join.sh
  scripts/commit-mz029-incremental-interval-join.sh
  scripts/push-mz029-incremental-interval-join.sh
)
expected_paths_list=("${feature_paths[@]}" Makefile)

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing to mix MZ-029 interval join with pre-existing staged changes' >&2
  exit 1
fi

makefile_block=(
  ''
  '.PHONY: test-mz029-incremental-interval-join benchmark-mz029-incremental-interval-join format-mz029-incremental-interval-join race-mz029-incremental-interval-join vet-mz029-incremental-interval-join review-mz029-incremental-interval-join verify-mz029-incremental-interval-join commit-mz029-incremental-interval-join push-mz029-incremental-interval-join'
  'test-mz029-incremental-interval-join:'
  $'\t@bash scripts/test-mz029-incremental-interval-join.sh'
  'benchmark-mz029-incremental-interval-join:'
  $'\t@bash scripts/benchmark-mz029-incremental-interval-join.sh'
  ''
  'format-mz029-incremental-interval-join:'
  $'\t@bash scripts/format-mz029-incremental-interval-join.sh'
  ''
  'race-mz029-incremental-interval-join:'
  $'\t@bash scripts/race-mz029-incremental-interval-join.sh'
  ''
  'vet-mz029-incremental-interval-join:'
  $'\t@bash scripts/vet-mz029-incremental-interval-join.sh'
  ''
  'review-mz029-incremental-interval-join:'
  $'\t@bash scripts/review-mz029-incremental-interval-join.sh'
  ''
  'verify-mz029-incremental-interval-join:'
  $'\t@bash scripts/verify-mz029-incremental-interval-join.sh'
  ''
  'commit-mz029-incremental-interval-join:'
  $'\t@bash scripts/commit-mz029-incremental-interval-join.sh'
  ''
  'push-mz029-incremental-interval-join:'
  $'\t@bash scripts/push-mz029-incremental-interval-join.sh'
)

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
git show HEAD:Makefile > "$tmp_dir/base-Makefile"
printf '%s\n' "${makefile_block[@]}" > "$tmp_dir/MZ029-Makefile-block"
base_lines="$(wc -l < "$tmp_dir/base-Makefile")"
block_lines="$(wc -l < "$tmp_dir/MZ029-Makefile-block")"
{
  printf '%s\n' 'diff --git a/Makefile b/Makefile'
  printf '%s\n' '--- a/Makefile'
  printf '%s\n' '+++ b/Makefile'
  printf '@@ -%s,0 +%s,%s @@\n' "$base_lines" "$base_lines" "$block_lines"
  while IFS= read -r line || [[ -n "$line" ]]; do
    printf '+%s\n' "$line"
  done < "$tmp_dir/MZ029-Makefile-block"
} > "$tmp_dir/MZ029-Makefile.patch"

git add -- "${feature_paths[@]}"
git apply --cached "$tmp_dir/MZ029-Makefile.patch"

staged_paths="$(git diff --cached --name-only)"
expected_paths="$(printf '%s\n' "${expected_paths_list[@]}" | sort)"
if [[ "$staged_paths" != "$expected_paths" ]]; then
  printf '%s\n' 'staged path set does not match the MZ-029 interval-join feature' >&2
  printf '%s\n' 'staged:' "$staged_paths" >&2
  printf '%s\n' 'expected:' "$expected_paths" >&2
  exit 1
fi
git diff --cached --check
git commit -m "$commit_message"
