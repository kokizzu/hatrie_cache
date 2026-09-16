#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  CHU11_AUTOMATIC_DATA_SKIPPING_INDEX_SELECTION.md
  Makefile
  PRODUCT_IDEA_GAPS.md
  README.md
  hat/hatSql/ch_u11_skip_index_advisor_benchmark_test.go
  hat/hatSql/ch_u11_skip_index_advisor_test.go
  hat/hatSql/index_advisor.go
  hat/hatSql/index_advisor_persistence.go
  hat/hatSql/index_advisor_persistence_test.go
  scripts/benchmark-chu11-before-c251.sh
  scripts/benchmark-chu11-c251.sh
  scripts/commit-chu11-c251.sh
  scripts/format-chu11-c251.sh
  scripts/push-chu11-c251.sh
  scripts/race-chu11-clean-c251.sh
  scripts/review-chu11-c251.sh
  scripts/stage-chu11-c251.sh
  scripts/test-chu11-c251.sh
  scripts/test-chu11-clean-c251.sh
  scripts/test-chu11-package-clean-c251.sh
  scripts/verify-chu11-docs-c251.sh
  scripts/vet-chu11-clean-c251.sh
)

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing to stage CH-U11 with pre-existing staged changes' >&2
  exit 1
fi

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu11-stage-XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

git show HEAD:Makefile > "$tmp_dir/Makefile.base"
cp "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.candidate"
awk '/^# CHU11 targets$/,/^# End CHU11 targets$/' Makefile > "$tmp_dir/ch-u11-targets"
if [[ ! -s "$tmp_dir/ch-u11-targets" ]]; then
  printf '%s\n' 'CHU11 Makefile target block is missing' >&2
  exit 1
fi
cat "$tmp_dir/ch-u11-targets" >> "$tmp_dir/Makefile.candidate"

set +e
diff -u "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.candidate" > "$tmp_dir/Makefile.patch"
diff_status=$?
set -e
if [[ "$diff_status" -ne 1 ]]; then
  printf 'unexpected Makefile diff status: %s\n' "$diff_status" >&2
  exit 1
fi
sed -i '1c\\--- a/Makefile' "$tmp_dir/Makefile.patch"
sed -i '2c\\+++ b/Makefile' "$tmp_dir/Makefile.patch"
git apply --cached "$tmp_dir/Makefile.patch"

paths_without_makefile=()
for path in "${feature_paths[@]}"; do
  if [[ "$path" != "Makefile" ]]; then
    paths_without_makefile+=("$path")
  fi
done
git add -- "${paths_without_makefile[@]}"

printf '%s\n' "${feature_paths[@]}" | sort > "$tmp_dir/expected"
git diff --cached --name-only | sort > "$tmp_dir/actual"
if ! cmp -s "$tmp_dir/expected" "$tmp_dir/actual"; then
  printf '%s\n' 'staged path set does not match the CH-U11 allowlist' >&2
  diff -u "$tmp_dir/expected" "$tmp_dir/actual" >&2 || true
  exit 1
fi
git diff --cached --check
