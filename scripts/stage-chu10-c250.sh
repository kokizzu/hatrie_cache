#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing to stage CH-U10 with an existing index' >&2
  exit 1
fi

feature_paths=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  CHU10_FEEDBACK_PROJECTION_SELECTION.md
  PRODUCT_IDEA_GAPS.md
  README.md
  hat/hatSql/projection_advisor.go
  hat/hatSql/ch_u10_projection_feedback_benchmark_test.go
  hat/hatSql/ch_u10_projection_feedback_integration_test.go
  hat/hatSql/ch_u10_projection_feedback_test.go
  scripts/benchmark-chu10-before-c250.sh
  scripts/benchmark-chu10-c250.sh
  scripts/commit-chu10-c250.sh
  scripts/format-chu10-c250.sh
  scripts/push-chu10-c250.sh
  scripts/race-chu10-c250.sh
  scripts/review-chu10-c250.sh
  scripts/stage-chu10-c250.sh
  scripts/test-chu10-c250.sh
  scripts/test-chu10-package-c250.sh
  scripts/test-chu10-repo-c250.sh
  scripts/verify-chu10-docs-c250.sh
  scripts/vet-chu10-c250.sh
)

tmp_dir=$(mktemp -d /tmp/hatrie-cache-stage-chu10.XXXXXX)
trap 'rm -rf "$tmp_dir"' EXIT
base="$tmp_dir/Makefile.base"
candidate="$tmp_dir/Makefile.candidate"
patch_file="$tmp_dir/Makefile.patch"

git show HEAD:Makefile > "$base"
cp "$base" "$candidate"
cat >> "$candidate" <<'EOF'

.PHONY: benchmark-chu10-before-c250
benchmark-chu10-before-c250:
	@bash ./scripts/benchmark-chu10-before-c250.sh

.PHONY: test-chu10-c250
test-chu10-c250:
	@bash ./scripts/test-chu10-c250.sh

.PHONY: format-chu10-c250
format-chu10-c250:
	@bash ./scripts/format-chu10-c250.sh

.PHONY: benchmark-chu10-c250
benchmark-chu10-c250:
	@bash ./scripts/benchmark-chu10-c250.sh

.PHONY: test-chu10-package-c250
test-chu10-package-c250:
	@bash ./scripts/test-chu10-package-c250.sh

.PHONY: race-chu10-c250
race-chu10-c250:
	@bash ./scripts/race-chu10-c250.sh

.PHONY: vet-chu10-c250
vet-chu10-c250:
	@bash ./scripts/vet-chu10-c250.sh

.PHONY: test-chu10-repo-c250
test-chu10-repo-c250:
	@bash ./scripts/test-chu10-repo-c250.sh

.PHONY: verify-chu10-docs-c250
verify-chu10-docs-c250:
	@bash ./scripts/verify-chu10-docs-c250.sh

.PHONY: review-chu10-c250
review-chu10-c250:
	@bash ./scripts/review-chu10-c250.sh

.PHONY: stage-chu10-c250
stage-chu10-c250:
	@bash ./scripts/stage-chu10-c250.sh

.PHONY: commit-chu10-c250
commit-chu10-c250:
	@bash ./scripts/commit-chu10-c250.sh

.PHONY: push-chu10-c250
push-chu10-c250:
	@bash ./scripts/push-chu10-c250.sh
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

printf '%s\n' 'staged CH-U10 files:'
git diff --cached --name-only
