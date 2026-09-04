#!/usr/bin/env bash
set -euo pipefail

files=(
  BENCHMARK.md
  hat/hatCache/command_allocation_budget_test.go
  scripts/benchmark-t155.sh
  scripts/commit-t155.sh
  scripts/format-t155.sh
  scripts/push-t155.sh
  scripts/review-t155.sh
  scripts/stage-t155.sh
  scripts/test-race-t155.sh
  scripts/test-t155.sh
  scripts/vet-t155.sh
)

staged=$(git diff --cached --name-only)
if [[ -n "$staged" ]]; then
  expected=$(printf '%s\n' Makefile "${files[@]}" | sort)
  if [[ "$staged" != "$expected" ]]; then
    echo "refusing to stage T155 with pre-existing staged changes" >&2
    git diff --cached --name-status >&2
    exit 1
  fi
  git reset -- Makefile "${files[@]}"
fi

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

git show :Makefile > "$tmp_dir/Makefile.base"
cp "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.t155"
cat >> "$tmp_dir/Makefile.t155" <<'MAKEFILE'

.PHONY: test-t155
test-t155:
	bash ./scripts/test-t155.sh

.PHONY: format-t155
format-t155:
	bash ./scripts/format-t155.sh

.PHONY: benchmark-t155
benchmark-t155:
	bash ./scripts/benchmark-t155.sh

.PHONY: test-race-t155
test-race-t155:
	bash ./scripts/test-race-t155.sh

.PHONY: vet-t155
vet-t155:
	bash ./scripts/vet-t155.sh

.PHONY: review-t155
review-t155:
	bash ./scripts/review-t155.sh

.PHONY: stage-t155
stage-t155:
	bash ./scripts/stage-t155.sh

.PHONY: commit-t155
commit-t155:
	bash ./scripts/commit-t155.sh

.PHONY: push-t155
push-t155:
	bash ./scripts/push-t155.sh
MAKEFILE

set +e
git diff --no-index -- "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.t155" > "$tmp_dir/Makefile.patch"
diff_status=$?
set -e
if [[ "$diff_status" -ne 1 ]]; then
  echo "unexpected Makefile diff status: $diff_status" >&2
  exit 1
fi

sed -i '1c diff --git a/Makefile b/Makefile' "$tmp_dir/Makefile.patch"
sed -i '3c --- a/Makefile' "$tmp_dir/Makefile.patch"
sed -i '4c +++ b/Makefile' "$tmp_dir/Makefile.patch"
git apply --cached -- "$tmp_dir/Makefile.patch"
git add -- "${files[@]}"

git diff --cached --check
echo "staged T155 paths:"
git diff --cached --name-only
