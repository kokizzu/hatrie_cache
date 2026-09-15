#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

allowed_files=(
  "ADOPTED_QUERY_ENGINE_IDEAS.md"
  "BENCHMARK.md"
  "CHU01_DURABLE_ASYNC_INSERT_DEDUP.md"
  "Makefile"
  "PRODUCT_IDEA_GAPS.md"
  "README.md"
  "hat/hatCache/ch009_async_insert_buffer.go"
  "hat/hatCache/ch009_async_insert_buffer_test.go"
  "hat/hatCache/chu01_durable_async_insert_benchmark_test.go"
  "hat/hatCache/chu01_durable_async_insert_test.go"
  "scripts/benchmark-chu01-after-c242.sh"
  "scripts/benchmark-chu01-before-c242.sh"
  "scripts/commit-chu01-c242.sh"
  "scripts/format-chu01-c242.sh"
  "scripts/push-chu01-c242.sh"
  "scripts/race-chu01-c242.sh"
  "scripts/stage-chu01-c242.sh"
  "scripts/test-chu01-c242.sh"
  "scripts/test-chu01-package-c242.sh"
  "scripts/vet-chu01-c242.sh"
)

is_allowed() {
  local candidate="$1"
  local allowed
  for allowed in "${allowed_files[@]}"; do
    if [[ "$candidate" == "$allowed" ]]; then
      return 0
    fi
  done
  return 1
}

git diff --cached --check
staged_count=0
while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  if ! is_allowed "$file"; then
    printf 'refusing to commit unrelated staged path: %s\n' "$file" >&2
    exit 1
  fi
  staged_count=$((staged_count + 1))
done < <(git diff --cached --name-only)
if [[ "$staged_count" -eq 0 ]]; then
  printf '%s\n' 'refusing to commit: no staged files' >&2
  exit 1
fi

git diff --cached --name-only
git commit -m "feat: deduplicate durable async inserts"
