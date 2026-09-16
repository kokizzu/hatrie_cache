#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu12-stage-XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT
staged_ok=true
if ! git diff --cached --quiet; then
  git diff --cached --name-only > "$tmp_dir/staged"
  while IFS= read -r path; do
    case "$path" in
      Makefile|ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md|CHU12_BACKGROUND_INDEX_REBUILD_QUEUE.md|PRODUCT_IDEA_GAPS.md|README.md|hat/hatSql/ch_u12_*|hat/hatSql/index_rebuild_queue.go|scripts/*chu12-c274.sh|scripts/verify-chu12-docs-c279.sh)
        ;;
      *)
        staged_ok=false
        ;;
    esac
  done < "$tmp_dir/staged"
  if [ "$staged_ok" != true ]; then
    printf '%s\n' 'Refusing to mix CH-U12 files with existing staged changes.'
    exit 1
  fi
fi

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CHU12_BACKGROUND_INDEX_REBUILD_QUEUE.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/ch_u12_index_rebuild_queue_baseline_benchmark_test.go \
  hat/hatSql/ch_u12_index_rebuild_queue_benchmark_test.go \
  hat/hatSql/ch_u12_index_rebuild_queue_test.go \
  hat/hatSql/index_rebuild_queue.go \
  scripts/benchmark-before-chu12-c274.sh \
  scripts/benchmark-chu12-c274.sh \
  scripts/commit-chu12-c274.sh \
  scripts/format-chu12-c274.sh \
  scripts/inspect-staged-chu12-c274.sh \
  scripts/push-chu12-c274.sh \
  scripts/review-chu12-c274.sh \
  scripts/stage-chu12-c274.sh \
  scripts/test-chu12-c274.sh \
  scripts/verify-chu12-c274.sh \
  scripts/verify-chu12-docs-c279.sh

git show HEAD:Makefile > "$tmp_dir/base.mk"
cp "$tmp_dir/base.mk" "$tmp_dir/feature.mk"
printf '%s\n' \
  '' \
  '# CHU12 test and baseline targets' \
  '.PHONY: test-chu12-c274 benchmark-before-chu12-c274' \
  'test-chu12-c274:' \
  $'\t@bash ./scripts/test-chu12-c274.sh' \
  'benchmark-before-chu12-c274:' \
  $'\t@bash ./scripts/benchmark-before-chu12-c274.sh' \
  '# End CHU12 test and baseline targets' \
  '' \
  '.PHONY: format-chu12-c274' \
  'format-chu12-c274:' \
  $'\t@bash ./scripts/format-chu12-c274.sh' \
  '' \
  '.PHONY: benchmark-chu12-c274' \
  'benchmark-chu12-c274:' \
  $'\t@bash ./scripts/benchmark-chu12-c274.sh' \
  '' \
  '.PHONY: verify-chu12-c274' \
  'verify-chu12-c274:' \
  $'\t@bash ./scripts/verify-chu12-c274.sh' \
  '' \
  '.PHONY: verify-chu12-docs-c279' \
  'verify-chu12-docs-c279:' \
  $'\t@bash ./scripts/verify-chu12-docs-c279.sh' \
  '' \
  '.PHONY: review-chu12-c274' \
  'review-chu12-c274:' \
  $'\t@bash ./scripts/review-chu12-c274.sh' \
  '' \
  '.PHONY: inspect-staged-chu12-c274' \
  'inspect-staged-chu12-c274:' \
  $'\t@bash ./scripts/inspect-staged-chu12-c274.sh' \
  '' \
  '.PHONY: stage-chu12-c274' \
  'stage-chu12-c274:' \
  $'\t@bash ./scripts/stage-chu12-c274.sh' \
  '' \
  '.PHONY: commit-chu12-c274' \
  'commit-chu12-c274:' \
  $'\t@bash ./scripts/commit-chu12-c274.sh' \
  '' \
  '.PHONY: push-chu12-c274' \
  'push-chu12-c274:' \
  $'\t@bash ./scripts/push-chu12-c274.sh' \
  '# End CHU12 delivery targets' >> "$tmp_dir/feature.mk"

diff_status=0
git diff --no-index --src-prefix=a/ --dst-prefix=b/ "$tmp_dir/base.mk" "$tmp_dir/feature.mk" > "$tmp_dir/makefile.patch" || diff_status=$?
if [ "$diff_status" -ne 1 ]; then
  printf 'Unexpected Makefile diff status: %s\n' "$diff_status"
  exit 1
fi
base_path=${tmp_dir#/}/base.mk
feature_path=${tmp_dir#/}/feature.mk
sed -i "s|a/$base_path|a/Makefile|; s|b/$feature_path|b/Makefile|" "$tmp_dir/makefile.patch"
if git diff --cached --quiet -- Makefile; then
  git apply --cached "$tmp_dir/makefile.patch"
fi
