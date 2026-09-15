#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

feature_files=(
  "ADOPTED_QUERY_ENGINE_IDEAS.md"
  "BENCHMARK.md"
  "CHU01_DURABLE_ASYNC_INSERT_DEDUP.md"
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

allowed_files=("Makefile" "${feature_files[@]}")
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
assert_staged_files_allowed() {
  local file
  while IFS= read -r file; do
    [[ -z "$file" ]] && continue
    if ! is_allowed "$file"; then
      printf 'refusing to stage with unrelated staged path: %s\n' "$file" >&2
      return 1
    fi
  done < <(git diff --cached --name-only)
}

assert_staged_files_allowed
git add -- "${feature_files[@]}"

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-chu01-stage.XXXXXX")"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

git show HEAD:Makefile > "$tmp_dir/Makefile.base"
cp "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.feature"
cat >> "$tmp_dir/Makefile.feature" <<'MAKEFILE_TARGETS'

.PHONY: test-chu01-c242 benchmark-chu01-before-c242 benchmark-chu01-after-c242 format-chu01-c242 test-chu01-package-c242 race-chu01-c242 vet-chu01-c242 stage-chu01-c242 commit-chu01-c242 push-chu01-c242
test-chu01-c242:
	bash ./scripts/test-chu01-c242.sh

benchmark-chu01-before-c242:
	bash ./scripts/benchmark-chu01-before-c242.sh

benchmark-chu01-after-c242:
	bash ./scripts/benchmark-chu01-after-c242.sh

format-chu01-c242:
	bash ./scripts/format-chu01-c242.sh

test-chu01-package-c242:
	bash ./scripts/test-chu01-package-c242.sh

race-chu01-c242:
	bash ./scripts/race-chu01-c242.sh

vet-chu01-c242:
	bash ./scripts/vet-chu01-c242.sh

stage-chu01-c242:
	bash ./scripts/stage-chu01-c242.sh

commit-chu01-c242:
	bash ./scripts/commit-chu01-c242.sh

push-chu01-c242:
	bash ./scripts/push-chu01-c242.sh
MAKEFILE_TARGETS

if diff -u --label a/Makefile --label b/Makefile "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.feature" > "$tmp_dir/Makefile.patch"; then
  printf '%s\n' 'Makefile already contains the CH-U01 targets'
else
  diff_status=$?
  if [[ "$diff_status" -ne 1 ]]; then
    exit "$diff_status"
  fi
  git apply --cached --check "$tmp_dir/Makefile.patch"
  git apply --cached "$tmp_dir/Makefile.patch"
fi

git diff --cached --check
assert_staged_files_allowed
git diff --cached --name-only
