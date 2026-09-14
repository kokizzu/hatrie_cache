#!/usr/bin/env bash
set -euo pipefail

commit_message="${1:-feat(hatSql): add skew-aware join exchange}"
feature_paths=(
  MZ031_SKEW_AWARE_JOIN_EXCHANGE.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  README.md
  hat/hatSql/m031_skew_aware_join_exchange.go
  hat/hatSql/m031_skew_aware_join_exchange_test.go
  hat/hatSql/m031_skew_aware_join_exchange_benchmark_test.go
  scripts/test-mz031-skew-aware-join-exchange.sh
  scripts/benchmark-mz031-skew-aware-join-exchange.sh
  scripts/format-mz031-skew-aware-join-exchange.sh
  scripts/race-mz031-skew-aware-join-exchange.sh
  scripts/vet-mz031-skew-aware-join-exchange.sh
  scripts/review-mz031-skew-aware-join-exchange.sh
  scripts/verify-mz031-skew-aware-join-exchange.sh
  scripts/commit-mz031-skew-aware-join-exchange.sh
  scripts/push-mz031-skew-aware-join-exchange.sh
)
expected_paths_list=("${feature_paths[@]}" Makefile)

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing to mix MZ-031 with pre-existing staged changes' >&2
  exit 1
fi

makefile_block=(
  ''
  '.PHONY: test-mz031-skew-aware-join-exchange benchmark-mz031-skew-aware-join-exchange format-mz031-skew-aware-join-exchange race-mz031-skew-aware-join-exchange vet-mz031-skew-aware-join-exchange review-mz031-skew-aware-join-exchange verify-mz031-skew-aware-join-exchange commit-mz031-skew-aware-join-exchange push-mz031-skew-aware-join-exchange'
  'test-mz031-skew-aware-join-exchange:'
  $'\t@bash scripts/test-mz031-skew-aware-join-exchange.sh'
  'benchmark-mz031-skew-aware-join-exchange:'
  $'\t@bash scripts/benchmark-mz031-skew-aware-join-exchange.sh'
  ''
  'format-mz031-skew-aware-join-exchange:'
  $'\t@bash scripts/format-mz031-skew-aware-join-exchange.sh'
  ''
  'race-mz031-skew-aware-join-exchange:'
  $'\t@bash scripts/race-mz031-skew-aware-join-exchange.sh'
  ''
  'vet-mz031-skew-aware-join-exchange:'
  $'\t@bash scripts/vet-mz031-skew-aware-join-exchange.sh'
  ''
  'review-mz031-skew-aware-join-exchange:'
  $'\t@bash scripts/review-mz031-skew-aware-join-exchange.sh'
  ''
  'verify-mz031-skew-aware-join-exchange:'
  $'\t@bash scripts/verify-mz031-skew-aware-join-exchange.sh'
  ''
  'commit-mz031-skew-aware-join-exchange:'
  $'\t@bash scripts/commit-mz031-skew-aware-join-exchange.sh'
  ''
  'push-mz031-skew-aware-join-exchange:'
  $'\t@bash scripts/push-mz031-skew-aware-join-exchange.sh'
)

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
git show HEAD:Makefile > "$tmp_dir/base-Makefile"
printf '%s\n' "${makefile_block[@]}" > "$tmp_dir/MZ031-Makefile-block"
base_lines="$(wc -l < "$tmp_dir/base-Makefile")"
block_lines="$(wc -l < "$tmp_dir/MZ031-Makefile-block")"
{
  printf '%s\n' 'diff --git a/Makefile b/Makefile'
  printf '%s\n' '--- a/Makefile'
  printf '%s\n' '+++ b/Makefile'
  printf '@@ -%s,0 +%s,%s @@\n' "$base_lines" "$base_lines" "$block_lines"
  while IFS= read -r line || [[ -n "$line" ]]; do
    printf '+%s\n' "$line"
  done < "$tmp_dir/MZ031-Makefile-block"
} > "$tmp_dir/MZ031-Makefile.patch"

git add -- "${feature_paths[@]}"
git apply --cached "$tmp_dir/MZ031-Makefile.patch"

staged_paths="$(git diff --cached --name-only)"
expected_paths="$(printf '%s\n' "${expected_paths_list[@]}" | sort)"
if [[ "$staged_paths" != "$expected_paths" ]]; then
  printf '%s\n' 'staged path set does not match the MZ-031 feature' >&2
  printf '%s\n' 'staged:' "$staged_paths" >&2
  printf '%s\n' 'expected:' "$expected_paths" >&2
  exit 1
fi
git diff --cached --check
git commit -m "$commit_message"
