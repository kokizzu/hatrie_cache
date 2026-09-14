#!/usr/bin/env bash
set -euo pipefail

paths=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  C209_TYPED_TABLE_STATS.md
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
  ENGINE_IDEAS.md
  hat/hatSql/c209_typed_table_stats_test.go
  hat/hatSql/typed_table.go
  hat/hatSql/typed_table_stats.go
  scripts/benchmark-tt050-typed-table-cache.sh
  scripts/commit-tt050-typed-table-cache.sh
  scripts/format-tt050-typed-table-cache.sh
  scripts/push-tt050-typed-table-cache.sh
  scripts/race-tt050-typed-table-cache.sh
  scripts/test-tt050-typed-table-cache.sh
  scripts/verify-tt050-typed-table-cache.sh
  scripts/vet-tt050-typed-table-cache.sh
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
    '@@ -13839,2 +13839,34 @@' \
    ' push-differential-average:'
  printf ' %s\n' $'\t@bash scripts/push-differential-average.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: test-tt050-typed-table-cache' \
    '+test-tt050-typed-table-cache:'
  printf '+%s\n' $'\t@bash scripts/test-tt050-typed-table-cache.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: benchmark-tt050-typed-table-cache' \
    '+benchmark-tt050-typed-table-cache:'
  printf '+%s\n' $'\t@bash scripts/benchmark-tt050-typed-table-cache.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: format-tt050-typed-table-cache' \
    '+format-tt050-typed-table-cache:'
  printf '+%s\n' $'\t@bash scripts/format-tt050-typed-table-cache.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: race-tt050-typed-table-cache' \
    '+race-tt050-typed-table-cache:'
  printf '+%s\n' $'\t@bash scripts/race-tt050-typed-table-cache.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: vet-tt050-typed-table-cache' \
    '+vet-tt050-typed-table-cache:'
  printf '+%s\n' $'\t@bash scripts/vet-tt050-typed-table-cache.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: verify-tt050-typed-table-cache' \
    '+verify-tt050-typed-table-cache:'
  printf '+%s\n' $'\t@bash scripts/verify-tt050-typed-table-cache.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: commit-tt050-typed-table-cache' \
    '+commit-tt050-typed-table-cache:'
  printf '+%s\n' $'\t@bash scripts/commit-tt050-typed-table-cache.sh'
  printf '%s\n' \
    '+' \
    '+.PHONY: push-tt050-typed-table-cache' \
    '+push-tt050-typed-table-cache:'
  printf '+%s\n' $'\t@bash scripts/push-tt050-typed-table-cache.sh'
} >"$patch_file"
git apply --cached "$patch_file"

git diff --cached --check
git commit -m 'feat(hatSql): cache typed-table statistics'
