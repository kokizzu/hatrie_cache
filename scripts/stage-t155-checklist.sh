#!/usr/bin/env bash
set -euo pipefail

files=(
  INSPIRATION.md
  scripts/commit-t155-checklist.sh
  scripts/push-t155-checklist.sh
  scripts/review-t155-checklist.sh
  scripts/stage-t155-checklist.sh
)

if ! git diff --cached --quiet; then
  echo "refusing to stage checklist correction with pre-existing staged changes" >&2
  git diff --cached --name-status >&2
  exit 1
fi

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

git show :Makefile > "$tmp_dir/Makefile.base"
cp "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.checklist"
cat >> "$tmp_dir/Makefile.checklist" <<'MAKEFILE'

.PHONY: review-t155-checklist
review-t155-checklist:
	bash ./scripts/review-t155-checklist.sh

.PHONY: stage-t155-checklist
stage-t155-checklist:
	bash ./scripts/stage-t155-checklist.sh

.PHONY: commit-t155-checklist
commit-t155-checklist:
	bash ./scripts/commit-t155-checklist.sh

.PHONY: push-t155-checklist
push-t155-checklist:
	bash ./scripts/push-t155-checklist.sh
MAKEFILE

set +e
git diff --no-index -- "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.checklist" > "$tmp_dir/Makefile.patch"
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
git diff --cached --name-only
