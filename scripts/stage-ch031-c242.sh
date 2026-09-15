#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

feature_files=(
  "ADOPTED_QUERY_ENGINE_IDEAS.md"
  "BENCHMARK.md"
  "CH031_TYPED_JSON_SUBCOLUMNS.md"
  "PRODUCT_IDEA_GAPS.md"
  "README.md"
  "hat/hatSql/ch031_typed_json_subcolumn_benchmark_test.go"
  "hat/hatSql/ch031_typed_json_subcolumn_baseline_benchmark_test.go"
  "hat/hatSql/ch031_typed_json_subcolumn_test.go"
  "hat/hatSql/columnar_json_subcolumn.go"
  "hat/hatSql/columnar_json_subcolumn_scan.go"
  "hat/hatSql/contracts.go"
  "hat/hatSql/json_path.go"
  "hat/hatSql/query.go"
  "scripts/benchmark-ch031-after-c242.sh"
  "scripts/benchmark-ch031-before-c242.sh"
  "scripts/commit-ch031-c242.sh"
  "scripts/format-ch031-c242.sh"
  "scripts/push-ch031-c242.sh"
  "scripts/race-ch031-c242.sh"
  "scripts/stage-ch031-c242.sh"
  "scripts/test-ch031-c242.sh"
  "scripts/test-ch031-package-c242.sh"
  "scripts/test-ch031-repo-c242.sh"
  "scripts/vet-ch031-c242.sh"
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

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-ch031-stage.XXXXXX")"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

git show HEAD:Makefile > "$tmp_dir/Makefile.base"
cp "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.feature"
cat >> "$tmp_dir/Makefile.feature" <<'MAKEFILE_TARGETS'

.PHONY: test-ch031-c242 benchmark-ch031-before-c242 benchmark-ch031-after-c242 format-ch031-c242 test-ch031-package-c242 race-ch031-c242 vet-ch031-c242 test-ch031-repo-c242 stage-ch031-c242 commit-ch031-c242 push-ch031-c242
test-ch031-c242:
	bash ./scripts/test-ch031-c242.sh

benchmark-ch031-before-c242:
	bash ./scripts/benchmark-ch031-before-c242.sh

benchmark-ch031-after-c242:
	bash ./scripts/benchmark-ch031-after-c242.sh

format-ch031-c242:
	bash ./scripts/format-ch031-c242.sh

test-ch031-package-c242:
	bash ./scripts/test-ch031-package-c242.sh

race-ch031-c242:
	bash ./scripts/race-ch031-c242.sh

vet-ch031-c242:
	bash ./scripts/vet-ch031-c242.sh

test-ch031-repo-c242:
	bash ./scripts/test-ch031-repo-c242.sh

stage-ch031-c242:
	bash ./scripts/stage-ch031-c242.sh

commit-ch031-c242:
	bash ./scripts/commit-ch031-c242.sh

push-ch031-c242:
	bash ./scripts/push-ch031-c242.sh
MAKEFILE_TARGETS

if diff -u --label a/Makefile --label b/Makefile "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.feature" > "$tmp_dir/Makefile.patch"; then
  printf '%s\n' 'Makefile already contains the CH-031 targets'
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
