#!/usr/bin/env bash
set -euo pipefail

paths=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  C210_TYPED_TABLE_HISTOGRAM.md
  ENGINE_IDEAS.md
  hat/hatSql/c210_typed_table_histogram_test.go
  hat/hatSql/typed_table.go
  hat/hatSql/typed_table_histogram.go
  scripts/benchmark-tt051-typed-table-histogram-cache.sh
  scripts/commit-tt051-typed-table-histogram-cache.sh
  scripts/format-tt051-typed-table-histogram-cache.sh
  scripts/push-tt051-typed-table-histogram-cache.sh
  scripts/race-tt051-typed-table-histogram-cache.sh
  scripts/test-tt051-typed-table-histogram-cache.sh
  scripts/verify-tt051-typed-table-histogram-cache.sh
  scripts/vet-tt051-typed-table-histogram-cache.sh
)

if ! git diff --cached --quiet; then
  echo 'refusing to commit with pre-existing staged changes' >&2
  exit 1
fi

for path in "${paths[@]}"; do
  if [[ ! -e $path ]]; then
    echo "expected feature path is missing: $path" >&2
    exit 1
  fi
done

git add "${paths[@]}"
git diff --cached --check

patch_file=$(mktemp)
trap 'rm -f "$patch_file"' EXIT
{
  printf '%s\n' \
    'diff --git a/Makefile b/Makefile' \
    '--- a/Makefile' \
    '+++ b/Makefile' \
    '@@ -13868,2 +13868,34 @@' \
    ' push-tt050-typed-table-cache:'
  printf ' %s\n' $'\t@bash scripts/push-tt050-typed-table-cache.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: test-tt051-typed-table-histogram-cache' \
    '+test-tt051-typed-table-histogram-cache:'
  printf '+%s\n' $'\t@bash scripts/test-tt051-typed-table-histogram-cache.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: benchmark-tt051-typed-table-histogram-cache' \
    '+benchmark-tt051-typed-table-histogram-cache:'
  printf '+%s\n' $'\t@bash scripts/benchmark-tt051-typed-table-histogram-cache.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: format-tt051-typed-table-histogram-cache' \
    '+format-tt051-typed-table-histogram-cache:'
  printf '+%s\n' $'\t@bash scripts/format-tt051-typed-table-histogram-cache.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: race-tt051-typed-table-histogram-cache' \
    '+race-tt051-typed-table-histogram-cache:'
  printf '+%s\n' $'\t@bash scripts/race-tt051-typed-table-histogram-cache.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: vet-tt051-typed-table-histogram-cache' \
    '+vet-tt051-typed-table-histogram-cache:'
  printf '+%s\n' $'\t@bash scripts/vet-tt051-typed-table-histogram-cache.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: verify-tt051-typed-table-histogram-cache' \
    '+verify-tt051-typed-table-histogram-cache:'
  printf '+%s\n' $'\t@bash scripts/verify-tt051-typed-table-histogram-cache.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: commit-tt051-typed-table-histogram-cache' \
    '+commit-tt051-typed-table-histogram-cache:'
  printf '+%s\n' $'\t@bash scripts/commit-tt051-typed-table-histogram-cache.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: push-tt051-typed-table-histogram-cache' \
    '+push-tt051-typed-table-histogram-cache:'
  printf '+%s\n' $'\t@bash scripts/push-tt051-typed-table-histogram-cache.sh'
} >"$patch_file"
git apply --cached "$patch_file"

git diff --cached --check
git commit -m 'feat(hatSql): cache typed-table histograms'
