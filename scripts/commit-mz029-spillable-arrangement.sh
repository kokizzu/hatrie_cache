#!/usr/bin/env bash
set -euo pipefail

commit_message="${1:-feat(hatDataStructure): add spillable arrangement}"
feature_paths=(
  MZ029_SPILLABLE_ARRANGEMENT.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  README.md
  hat/hatDataStructure/spillable_arrangement.go
  hat/hatDataStructure/spillable_arrangement_test.go
  hat/hatDataStructure/spillable_arrangement_benchmark_test.go
  scripts/test-mz029-spillable-arrangement.sh
  scripts/benchmark-mz029-spillable-arrangement.sh
  scripts/format-mz029-spillable-arrangement.sh
  scripts/race-mz029-spillable-arrangement.sh
  scripts/vet-mz029-spillable-arrangement.sh
  scripts/review-mz029-spillable-arrangement.sh
  scripts/verify-mz029-spillable-arrangement.sh
  scripts/commit-mz029-spillable-arrangement.sh
  scripts/push-mz029-spillable-arrangement.sh
)
expected_paths_list=("${feature_paths[@]}" Makefile)

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing to mix MZ-029 spillable arrangement with pre-existing staged changes' >&2
  exit 1
fi

makefile_block=(
  ''
  '.PHONY: test-mz029-spillable-arrangement benchmark-mz029-spillable-arrangement format-mz029-spillable-arrangement race-mz029-spillable-arrangement vet-mz029-spillable-arrangement review-mz029-spillable-arrangement verify-mz029-spillable-arrangement commit-mz029-spillable-arrangement push-mz029-spillable-arrangement'
  'test-mz029-spillable-arrangement:'
  $'\t@bash scripts/test-mz029-spillable-arrangement.sh'
  'benchmark-mz029-spillable-arrangement:'
  $'\t@bash scripts/benchmark-mz029-spillable-arrangement.sh'
  ''
  'format-mz029-spillable-arrangement:'
  $'\t@bash scripts/format-mz029-spillable-arrangement.sh'
  ''
  'race-mz029-spillable-arrangement:'
  $'\t@bash scripts/race-mz029-spillable-arrangement.sh'
  ''
  'vet-mz029-spillable-arrangement:'
  $'\t@bash scripts/vet-mz029-spillable-arrangement.sh'
  ''
  'review-mz029-spillable-arrangement:'
  $'\t@bash scripts/review-mz029-spillable-arrangement.sh'
  ''
  'verify-mz029-spillable-arrangement:'
  $'\t@bash scripts/verify-mz029-spillable-arrangement.sh'
  ''
  'commit-mz029-spillable-arrangement:'
  $'\t@bash scripts/commit-mz029-spillable-arrangement.sh'
  ''
  'push-mz029-spillable-arrangement:'
  $'\t@bash scripts/push-mz029-spillable-arrangement.sh'
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
  printf '%s\n' 'staged path set does not match the MZ-029 spillable-arrangement feature' >&2
  printf '%s\n' 'staged:' "$staged_paths" >&2
  printf '%s\n' 'expected:' "$expected_paths" >&2
  exit 1
fi
git diff --cached --check
git commit -m "$commit_message"
