#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

allowed_files=(
  "ADOPTED_QUERY_ENGINE_IDEAS.md"
  "BENCHMARK.md"
  "CH031_TYPED_JSON_SUBCOLUMNS.md"
  "Makefile"
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
git commit -m "feat: add typed JSON subcolumns"
