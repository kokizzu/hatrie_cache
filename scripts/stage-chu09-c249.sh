#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing to stage CH-U09 with an existing index' >&2
  exit 1
fi

feature_paths=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  CHU09_PERSISTED_SQL_RESULT_CACHE.md
  PRODUCT_IDEA_GAPS.md
  README.md
  hat/hatCache/sql_result_cache_auto.go
  hat/hatCache/sql_result_cache_persistence_test.go
  hat/hatSql/result_cache_persistence.go
  hat/hatSql/result_cache_persistence_benchmark_test.go
  hat/hatSql/result_cache_persistence_test.go
  scripts/benchmark-chu09-c249.sh
  scripts/format-chu09-c249.sh
  scripts/race-chu09-c249.sh
  scripts/review-chu09-c249.sh
  scripts/stage-chu09-c249.sh
  scripts/commit-chu09-c249.sh
  scripts/push-chu09-c249.sh
  scripts/test-chu09-c249.sh
  scripts/test-chu09-hatcache-c249.sh
  scripts/test-chu09-package-c249.sh
  scripts/test-chu09-repo-c249.sh
  scripts/verify-chu09-docs-c249.sh
  scripts/vet-chu09-c249.sh
)

tmp_dir=$(mktemp -d /tmp/hatrie-cache-stage-chu09.XXXXXX)
trap 'rm -rf "$tmp_dir"' EXIT
base="$tmp_dir/Makefile.base"
candidate="$tmp_dir/Makefile.candidate"
patch_file="$tmp_dir/Makefile.patch"

git show HEAD:Makefile > "$base"
cp "$base" "$candidate"
cat >> "$candidate" <<'EOF'

.PHONY: test-chu09-c249
test-chu09-c249:
	@bash ./scripts/test-chu09-c249.sh

.PHONY: format-chu09-c249
format-chu09-c249:
	@bash ./scripts/format-chu09-c249.sh

.PHONY: benchmark-chu09-c249
benchmark-chu09-c249:
	@bash ./scripts/benchmark-chu09-c249.sh

.PHONY: test-chu09-hatcache-c249
test-chu09-hatcache-c249:
	@bash ./scripts/test-chu09-hatcache-c249.sh

.PHONY: test-chu09-package-c249
test-chu09-package-c249:
	@bash ./scripts/test-chu09-package-c249.sh

.PHONY: race-chu09-c249
race-chu09-c249:
	@bash ./scripts/race-chu09-c249.sh

.PHONY: vet-chu09-c249
vet-chu09-c249:
	@bash ./scripts/vet-chu09-c249.sh

.PHONY: test-chu09-repo-c249
test-chu09-repo-c249:
	@bash ./scripts/test-chu09-repo-c249.sh

.PHONY: review-chu09-c249
review-chu09-c249:
	@bash ./scripts/review-chu09-c249.sh

.PHONY: verify-chu09-docs-c249
verify-chu09-docs-c249:
	@bash ./scripts/verify-chu09-docs-c249.sh

.PHONY: stage-chu09-c249
stage-chu09-c249:
	@bash ./scripts/stage-chu09-c249.sh

.PHONY: commit-chu09-c249
commit-chu09-c249:
	@bash ./scripts/commit-chu09-c249.sh

.PHONY: push-chu09-c249
push-chu09-c249:
	@bash ./scripts/push-chu09-c249.sh
EOF

diff_status=0
diff -u "$base" "$candidate" > "$patch_file" || diff_status=$?
if [[ "$diff_status" -ne 1 ]]; then
  printf 'unexpected Makefile diff status: %s\n' "$diff_status" >&2
  exit 1
fi
sed -i \
  -e 's|^--- .*|--- a/Makefile|' \
  -e 's|^+++ .*|+++ b/Makefile|' \
  "$patch_file"
git apply --cached "$patch_file"
git add -- "${feature_paths[@]}"

git diff --cached --check

expected=("${feature_paths[@]}" Makefile)
mapfile -t actual < <(git diff --cached --name-only)
if [[ "${#actual[@]}" -ne "${#expected[@]}" ]]; then
  printf 'unexpected staged path count: got %s want %s\n' "${#actual[@]}" "${#expected[@]}" >&2
  exit 1
fi
for path in "${expected[@]}"; do
  found=0
  for staged in "${actual[@]}"; do
    if [[ "$path" == "$staged" ]]; then
      found=1
      break
    fi
  done
  if [[ "$found" -eq 0 ]]; then
    printf 'missing expected staged path: %s\n' "$path" >&2
    exit 1
  fi
done

printf '%s\n' 'staged CH-U09 files:'
git diff --cached --name-only
