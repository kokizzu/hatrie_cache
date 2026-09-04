#!/usr/bin/env bash
set -euo pipefail

feature_files=(
  BENCHMARK.md
  INSPIRATION.md
  README.md
  cmd/hatrie-cache/main.go
  cmd/hatrie-cache/main_test.go
  hat/hatCache/leveldb_store.go
  hat/hatCache/pebble_generation.go
  hat/hatCache/pebble_store.go
  hat/hatCache/storage_size_limit.go
  hat/hatCache/storage_size_limit_test.go
  persistent_storage_size_limit_api.go
  scripts/test-t111.sh
  scripts/format-t111.sh
  scripts/benchmark-t111.sh
  scripts/test-race-t111.sh
  scripts/vet-t111.sh
  scripts/review-t111.sh
  scripts/stage-t111.sh
  scripts/commit-t111.sh
  scripts/push-t111.sh
)

git add -- "${feature_files[@]}"

index_file=$(mktemp)
desired_file=$(mktemp)
patch_file=$(mktemp)
trap 'rm -f "$index_file" "$desired_file" "$patch_file"' EXIT

git show :Makefile > "$index_file"
if ! grep -q '^test-t111:' "$index_file"; then
  cp "$index_file" "$desired_file"
  printf '%s\n' \
    '' \
    'test-t111:' \
    $'\tbash ./scripts/test-t111.sh' \
    '' \
    'format-t111:' \
    $'\tbash ./scripts/format-t111.sh' \
    '' \
    'benchmark-t111:' \
    $'\tbash ./scripts/benchmark-t111.sh' \
    '' \
    'test-race-t111:' \
    $'\tbash ./scripts/test-race-t111.sh' \
    '' \
    'vet-t111:' \
    $'\tbash ./scripts/vet-t111.sh' \
    '' \
    'review-t111:' \
    $'\tbash ./scripts/review-t111.sh' \
    '' \
    'stage-t111:' \
    $'\tbash ./scripts/stage-t111.sh' \
    '' \
    'commit-t111:' \
    $'\tbash ./scripts/commit-t111.sh' \
    '' \
    'push-t111:' \
    $'\tbash ./scripts/push-t111.sh' >> "$desired_file"
  diff -u --label a/Makefile --label b/Makefile "$index_file" "$desired_file" > "$patch_file" || diff_status=$?
  if [[ "${diff_status:-0}" -ne 1 ]]; then
    printf 'unable to create the Makefile staging patch (status %s)\n' "${diff_status:-0}" >&2
    exit "${diff_status:-1}"
  fi
  git apply --cached "$patch_file"
fi

git diff --cached --check
