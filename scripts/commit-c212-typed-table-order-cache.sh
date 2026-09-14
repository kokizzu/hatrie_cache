#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  C212_TYPED_TABLE_ORDER_CACHE.md
  ENGINE_IDEAS.md
  README.md
  TYPED_TABLES.md
  hat/hatSql/c212_typed_table_order_benchmark_test.go
  hat/hatSql/c212_typed_table_order_test.go
  hat/hatSql/catalog.go
  hat/hatSql/session.go
  hat/hatSql/typed_table.go
  hat/hatSql/typed_table_columnar_order.go
  scripts/benchmark-c212-typed-table-order-cache.sh
  scripts/commit-c212-typed-table-order-cache.sh
  scripts/format-c212-typed-table-order-cache.sh
  scripts/push-c212-typed-table-order-cache.sh
  scripts/race-c212-typed-table-order-cache.sh
  scripts/review-c212-typed-table-order-cache.sh
  scripts/test-c212-typed-table-order-cache.sh
  scripts/verify-c212-typed-table-order-cache.sh
  scripts/vet-c212-typed-table-order-cache.sh
)

head_makefile=$(mktemp)
git show HEAD:Makefile > "$head_makefile"
if rg -q '^\.PHONY: benchmark-c212-typed-table-order-cache$' "$head_makefile"; then
  rm -f "$head_makefile"
  git add -- scripts/commit-c212-typed-table-order-cache.sh scripts/push-c212-typed-table-order-cache.sh
  git diff --cached --check
  git commit --amend --no-edit
  exit 0
fi
rm -f "$head_makefile"

if ! git diff --cached --quiet; then
  printf '%s\n' 'Refusing to commit: the index already contains staged changes.' >&2
  git diff --cached --name-only >&2
  exit 1
fi

for path in "${feature_paths[@]}"; do
  if [[ ! -e "$path" ]]; then
    printf 'Missing feature path: %s\n' "$path" >&2
    exit 1
  fi
done

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C212_TYPED_TABLE_ORDER_CACHE.md \
  ENGINE_IDEAS.md \
  README.md \
  TYPED_TABLES.md \
  hat/hatSql/c212_typed_table_order_benchmark_test.go \
  hat/hatSql/c212_typed_table_order_test.go \
  hat/hatSql/catalog.go \
  hat/hatSql/session.go \
  hat/hatSql/typed_table.go \
  hat/hatSql/typed_table_columnar_order.go \
  scripts/benchmark-c212-typed-table-order-cache.sh \
  scripts/commit-c212-typed-table-order-cache.sh \
  scripts/format-c212-typed-table-order-cache.sh \
  scripts/push-c212-typed-table-order-cache.sh \
  scripts/race-c212-typed-table-order-cache.sh \
  scripts/review-c212-typed-table-order-cache.sh \
  scripts/test-c212-typed-table-order-cache.sh \
  scripts/verify-c212-typed-table-order-cache.sh \
  scripts/vet-c212-typed-table-order-cache.sh

working_makefile=$(mktemp)
candidate_makefile=$(mktemp)
actual_block=$(mktemp)
expected_block=$(mktemp)
cleanup() {
  local status=$?
  if [[ -f "$working_makefile" ]]; then
    cp "$working_makefile" Makefile
  fi
  rm -f "$working_makefile" "$candidate_makefile" "$actual_block" "$expected_block"
  exit "$status"
}
trap cleanup EXIT

cp Makefile "$working_makefile"
awk '
  /^\.PHONY: benchmark-c212-typed-table-order-cache$/ { capture = 1 }
  capture { print }
' "$working_makefile" > "$actual_block"
printf '%s\n' \
  '.PHONY: benchmark-c212-typed-table-order-cache' \
  'benchmark-c212-typed-table-order-cache:' \
  $'\t@bash scripts/benchmark-c212-typed-table-order-cache.sh' \
  '' \
  '.PHONY: test-c212-typed-table-order-cache' \
  'test-c212-typed-table-order-cache:' \
  $'\t@bash scripts/test-c212-typed-table-order-cache.sh' \
  '' \
  '.PHONY: format-c212-typed-table-order-cache' \
  'format-c212-typed-table-order-cache:' \
  $'\t@bash scripts/format-c212-typed-table-order-cache.sh' \
  '' \
  '.PHONY: race-c212-typed-table-order-cache' \
  'race-c212-typed-table-order-cache:' \
  $'\t@bash scripts/race-c212-typed-table-order-cache.sh' \
  '' \
  '.PHONY: vet-c212-typed-table-order-cache' \
  'vet-c212-typed-table-order-cache:' \
  $'\t@bash scripts/vet-c212-typed-table-order-cache.sh' \
  '' \
  '.PHONY: review-c212-typed-table-order-cache' \
  'review-c212-typed-table-order-cache:' \
  $'\t@bash scripts/review-c212-typed-table-order-cache.sh' \
  '' \
  '.PHONY: verify-c212-typed-table-order-cache' \
  'verify-c212-typed-table-order-cache:' \
  $'\t@bash scripts/verify-c212-typed-table-order-cache.sh' \
  '' \
  '.PHONY: commit-c212-typed-table-order-cache' \
  'commit-c212-typed-table-order-cache:' \
  $'\t@bash scripts/commit-c212-typed-table-order-cache.sh' \
  '' \
  '.PHONY: push-c212-typed-table-order-cache' \
  'push-c212-typed-table-order-cache:' \
  $'\t@bash scripts/push-c212-typed-table-order-cache.sh' \
  > "$expected_block"
if ! cmp -s "$actual_block" "$expected_block"; then
  printf '%s\n' 'C212 Makefile block is missing or has unexpected trailing changes.' >&2
  exit 1
fi

git show HEAD:Makefile > "$candidate_makefile"
if rg -q '^\.PHONY: benchmark-c212-typed-table-order-cache$' "$candidate_makefile"; then
  printf '%s\n' 'C212 Makefile targets are already in HEAD; refusing an ambiguous stage.' >&2
  exit 1
fi
printf '\n' >> "$candidate_makefile"
awk '{ print }' "$actual_block" >> "$candidate_makefile"
cp "$candidate_makefile" Makefile
git add -- Makefile
cp "$working_makefile" Makefile

git diff --cached --check
git commit -m 'perf(hatSql): cache typed-table sorted ordinals'
